defmodule BroadwayCloudPubSub.GoogleApiClient do
  @moduledoc """
  Default Pub/Sub client used by `BroadwayCloudPubSub.Producer` to communicate with Google
  Cloud Pub/Sub service. This client implements the `BroadwayCloudPubSub.Client` behaviour
  which defines callbacks for receiving and acknowledging messages.
  """

  import GoogleApi.PubSub.V1.Api.Projects
  alias Broadway.{Message, Acknowledger}
  alias BroadwayCloudPubSub.Client
  alias GoogleApi.PubSub.V1.Connection

  alias GoogleApi.PubSub.V1.Model.{
    PullRequest,
    AcknowledgeRequest,
    ModifyAckDeadlineRequest,
    PubsubMessage
  }

  alias Tesla.Adapter.Hackney
  require Logger

  @behaviour Client
  @behaviour Acknowledger

  @default_max_number_of_messages 10

  @default_scope "https://www.googleapis.com/auth/pubsub"

  defp conn!(config, adapter_opts \\ []) do
    %{adapter: adapter, token_generator: {mod, fun, args}} = config

    {:ok, token} = apply(mod, fun, args)

    token
    |> Connection.new()
    |> override_tesla_adapter({adapter, adapter_opts})
  end

  defp override_tesla_adapter(client, adapter) do
    %{adapter: adapter} = Tesla.client([], adapter)
    %{client | adapter: adapter}
  end

  @impl Client
  def init(opts) do
    with {:ok, subscription} <- validate_subscription(opts),
         {:ok, token_generator} <- validate_token_opts(opts),
         {:ok, pull_request} <- validate_pull_request(opts),
         {:ok, on_success} <- validate(opts, :on_success, :ack),
         {:ok, on_failure} <- validate(opts, :on_failure, :ignore) do
      adapter = Keyword.get(opts, :__internal_tesla_adapter__, Hackney)

      storage_ref =
        Broadway.TermStorage.put(%{
          adapter: adapter,
          subscription: subscription,
          token_generator: token_generator
        })

      ack_ref = {__MODULE__, storage_ref}

      {:ok,
       %{
         adapter: adapter,
         subscription: subscription,
         token_generator: token_generator,
         pull_request: pull_request,
         ack_ref: ack_ref,
         on_success: on_success,
         on_failure: on_failure
       }}
    end
  end

  @impl Client
  def receive_messages(demand, opts) do
    pull_request = put_max_number_of_messages(opts.pull_request, demand)

    opts
    |> conn!(recv_timeout: :infinity)
    |> pubsub_projects_subscriptions_pull(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: pull_request
    )
    |> wrap_received_messages(opts.ack_ref, opts)
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    ack_messages(successful, ack_ref, :successful)
    ack_messages(failed, ack_ref, :failed)
  end

  @impl Acknowledger
  def configure(_channel, ack_data, options) do
    options = assert_valid_success_failure_opts!(options)
    ack_data = Map.merge(ack_data, Map.new(options))
    {:ok, ack_data}
  end

  defp assert_valid_success_failure_opts!(options) do
    Enum.map(options, fn
      {:on_success, value} -> {:on_success, validate_option!(:on_success, value)}
      {:on_failure, value} -> {:on_failure, validate_option!(:on_failure, value)}
      {other, _value} -> raise ArgumentError, "unsupported configure option #{inspect(other)}"
    end)
  end

  defp ack_messages([], _ref, _kind), do: :ok

  defp ack_messages(messages, {_pid, ref}, kind) do
    opts = Broadway.TermStorage.get!(ref)

    messages
    |> Enum.group_by(&group_acknowledger(&1, kind))
    |> Enum.map(fn {action, messages} ->
      action |> apply_ack_func(messages, opts) |> handle_acknowledged_messages()
    end)
  end

  defp group_acknowledger(%{acknowledger: {_mod, _chan, ack_data}}, kind) do
    ack_data_action(ack_data, kind)
  end

  defp ack_data_action(%{on_success: action}, :successful), do: action
  defp ack_data_action(%{on_failure: action}, :failed), do: action

  defp apply_ack_func(:ignore, _messages, _opts), do: :ok

  defp apply_ack_func(:ack, messages, opts) do
    ack_ids = Enum.map(messages, &extract_ack_id/1)

    opts
    |> conn!()
    |> pubsub_projects_subscriptions_acknowledge(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: %AcknowledgeRequest{ackIds: ack_ids}
    )
  end

  defp apply_ack_func(:no_ack, messages, opts),
    do: apply_ack_func({:no_ack, 0}, messages, opts)

  defp apply_ack_func({:no_ack, deadline}, messages, opts) do
    ack_ids = Enum.map(messages, &extract_ack_id/1)

    opts
    |> conn!()
    |> pubsub_projects_subscriptions_modify_ack_deadline(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: %ModifyAckDeadlineRequest{ackDeadlineSeconds: deadline, ackIds: ack_ids}
    )
  end

  defp handle_acknowledged_messages(:ok), do: :ok

  defp handle_acknowledged_messages({:ok, _}), do: :ok

  defp handle_acknowledged_messages({:error, reason}) do
    Logger.error("Unable to acknowledge messages with Cloud Pub/Sub. Reason: #{inspect(reason)}")
    :ok
  end

  defp wrap_received_messages({:ok, %{receivedMessages: received_messages}}, ack_ref, opts)
       when is_list(received_messages) do
    Enum.map(received_messages, fn %{message: message, ackId: ack_id} ->
      {data, metadata} =
        message
        |> decode_message()
        |> Map.from_struct()
        |> Map.pop(:data)

      ack_data = %{ack_id: ack_id, on_success: opts.on_success, on_failure: opts.on_failure}

      %Message{
        data: data,
        metadata: metadata,
        acknowledger: {__MODULE__, ack_ref, ack_data}
      }
    end)
  end

  defp wrap_received_messages({:ok, _}, _ack_ref, _opts) do
    []
  end

  defp wrap_received_messages({:error, reason}, _, _) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub. Reason: #{inspect(reason)}")
    []
  end

  defp decode_message(%PubsubMessage{data: nil} = message), do: message

  defp decode_message(%PubsubMessage{data: encoded_data} = message) do
    data = Base.decode64!(encoded_data)

    put_in(message.data, data)
  end

  defp put_max_number_of_messages(pull_request, demand) do
    max_number_of_messages = min(demand, pull_request.maxMessages)

    %{pull_request | maxMessages: max_number_of_messages}
  end

  defp extract_ack_id(message) do
    {_, _, %{ack_id: ack_id}} = message.acknowledger
    ack_id
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option!(key, value) do
    case validate_option(key, value) do
      {:ok, value} -> value
      {:error, message} -> raise ArgumentError, message
    end
  end

  defp validate_option(:token_generator, {m, f, args})
       when is_atom(m) and is_atom(f) and is_list(args) do
    {:ok, {m, f, args}}
  end

  defp validate_option(:token_generator, value),
    do: validation_error(:token_generator, "a tuple {Mod, Fun, Args}", value)

  defp validate_option(:scope, value) when not is_binary(value) or value == "",
    do: validation_error(:scope, "a non empty string", value)

  defp validate_option(:subscription, value) when not is_binary(value) or value == "",
    do: validation_error(:subscription, "a non empty string", value)

  defp validate_option(:max_number_of_messages, value) when not is_integer(value) or value < 1,
    do: validation_error(:max_number_of_messages, "a positive integer", value)

  defp validate_option(:return_immediately, nil), do: {:ok, nil}

  defp validate_option(:return_immediately, value) when not is_boolean(value),
    do: validation_error(:return_immediately, "a boolean value", value)

  defp validate_option(action, value) when action in [:on_success, :on_failure] do
    case validate_acking(value) do
      {:ok, result} -> {:ok, result}
      :error -> validation_error(action, "a valid #{action} value", value)
    end
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_acking(:ack), do: {:ok, :ack}
  defp validate_acking(:ignore), do: {:ok, :ignore}
  defp validate_acking(:no_ack), do: {:ok, {:no_ack, 0}}
  defp validate_acking({:no_ack, n}) when is_integer(n) and n >= 0, do: {:ok, {:no_ack, n}}
  defp validate_acking(_), do: :error

  defp validate_pull_request(opts) do
    with {:ok, return_immediately} <- validate(opts, :return_immediately),
         {:ok, max_number_of_messages} <-
           validate(opts, :max_number_of_messages, @default_max_number_of_messages) do
      {:ok,
       %PullRequest{
         maxMessages: max_number_of_messages,
         returnImmediately: return_immediately
       }}
    end
  end

  defp validate_token_opts(opts) do
    case Keyword.fetch(opts, :token_generator) do
      {:ok, _} -> validate(opts, :token_generator)
      :error -> validate_scope(opts)
    end
  end

  defp validate_scope(opts) do
    with {:ok, scope} <- validate(opts, :scope, @default_scope) do
      ensure_goth_loaded()
      {:ok, {__MODULE__, :generate_goth_token, [scope]}}
    end
  end

  defp ensure_goth_loaded() do
    unless Code.ensure_loaded?(Goth.Token) do
      Logger.error("""
      the default authentication token generator uses the Goth library but it's not available

      Add goth to your dependencies:

          defp deps() do
            {:goth, "~> 1.0"}
          end

      Or provide your own token generator:

          Broadway.start_link(
            producers: [
              default: [
                module: {BroadwayCloudPubSub.Producer,
                  token_generator: {MyGenerator, :generate, ["foo"]}
                }
              ]
            ]
          )
      """)
    end
  end

  defp validate_subscription(opts) do
    with {:ok, subscription} <- validate(opts, :subscription) do
      subscription |> String.split("/") |> validate_sub_parts(subscription)
    end
  end

  defp validate_sub_parts(
         ["projects", projects_id, "subscriptions", subscriptions_id],
         _subscription
       ) do
    {:ok, %{projects_id: projects_id, subscriptions_id: subscriptions_id}}
  end

  defp validate_sub_parts(_, subscription) do
    validation_error(:subscription, "an valid subscription name", subscription)
  end

  @doc false
  def generate_goth_token(scope) do
    with {:ok, %{token: token}} <- Goth.Token.for_scope(scope) do
      {:ok, token}
    end
  end
end
