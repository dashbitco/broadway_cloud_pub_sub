defmodule BroadwayCloudPubSub.GoogleApiClient do
  @moduledoc """
  Default Pub/Sub client used by `BroadwayCloudPubSub.Producer` to communicate with Google
  Cloud Pub/Sub service. This client implements the `BroadwayCloudPubSub.Client` behaviour
  which defines callbacks for receiving and acknowledging messages.
  """

  alias GoogleApi.PubSub.V1.Api.Projects
  alias Broadway.{Message, Acknowledger}
  alias BroadwayCloudPubSub.Client
  alias GoogleApi.PubSub.V1.Connection
  alias GoogleApi.PubSub.V1.Model.{PullRequest, AcknowledgeRequest}
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
         {:ok, pull_request} <- validate_pull_request(opts) do
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
         ack_ref: ack_ref
       }}
    end
  end

  @impl Client
  def receive_messages(demand, opts) do
    pull_request = put_max_number_of_messages(opts.pull_request, demand)

    opts
    |> conn!(recv_timeout: :infinity)
    |> Projects.pubsub_projects_subscriptions_pull(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: pull_request
    )
    |> wrap_received_messages(opts.ack_ref)
  end

  @impl Acknowledger
  def ack(ack_ref, successful, _failed) do
    successful
    |> acknowledge_messages(ack_ref)
  end

  defp acknowledge_messages([], _), do: :ok

  defp acknowledge_messages(messages, {_pid, ref}) do
    ack_ids = Enum.map(messages, &extract_ack_id/1)

    opts = Broadway.TermStorage.get!(ref)

    opts
    |> conn!()
    |> Projects.pubsub_projects_subscriptions_acknowledge(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: %AcknowledgeRequest{ackIds: ack_ids}
    )
    |> handle_acknowledged_messages()
  end

  defp handle_acknowledged_messages({:ok, _}), do: :ok

  defp handle_acknowledged_messages({:error, reason}) do
    Logger.error("Unable to acknowledge messages with Cloud Pub/Sub. Reason: #{inspect(reason)}")
    :ok
  end

  def wrap_received_messages({:ok, %{receivedMessages: received_messages}}, ack_ref)
      when is_list(received_messages) do
    Enum.map(
      received_messages,
      fn
        %{message: message, ackId: ack_id} -> to_message(message, ack_id, ack_ref)
        %{"message" => message, "ackId" => ack_id} -> to_message(message, ack_id, ack_ref)
      end
    )
  end

  def wrap_received_messages({:ok, _}, _ack_ref) do
    []
  end

  def wrap_received_messages({:error, reason}, _) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub. Reason: #{inspect(reason)}")
    []
  end

  defp to_message(message = %{"data" => nil}, ack_id, ack_ref) do
    no_data_message(message, ack_id, ack_ref)
  end

  defp to_message(message = %{data: nil}, ack_id, ack_ref) do
    no_data_message(message, ack_id, ack_ref)
  end

  defp to_message(message = %{data: data}, ack_id, ack_ref) do
    %Message{
      data: Base.decode64!(data),
      metadata: Map.take(message, [:attributes, :messageId, :publishTime]),
      acknowledger: {__MODULE__, ack_ref, ack_id}
    }
  end

  defp to_message(message = %{"data" => data}, ack_id, ack_ref) do
    %Message{
      data: Base.decode64!(data),
      metadata: Map.take(message, [:attributes, :messageId, :publishTime]),
      acknowledger: {__MODULE__, ack_ref, ack_id}
    }
  end

  defp no_data_message(message, ack_id, ack_ref) do
    %Message{
      data: nil,
      metadata: Map.take(message, [:attributes, :messageId, :publishTime]),
      acknowledger: {__MODULE__, ack_ref, ack_id}
    }
  end

  defp put_max_number_of_messages(pull_request, demand) do
    max_number_of_messages = min(demand, pull_request.maxMessages)

    %{pull_request | maxMessages: max_number_of_messages}
  end

  defp extract_ack_id(message) do
    {_, _, ack_id} = message.acknowledger
    ack_id
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
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

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

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
