defmodule BroadwayCloudPubSub.GoogleApiClient do
  @moduledoc """
  Default Pub/Sub client used by `BroadwayCloudPubSub.Producer` to communicate with Google
  Cloud Pub/Sub service. This client implements the `BroadwayCloudPubSub.Client` behaviour
  which defines callbacks for receiving and acknowledging messages.
  """

  import GoogleApi.PubSub.V1.Api.Projects
  alias Broadway.Message
  alias BroadwayCloudPubSub.{Client, ClientAcknowledger}
  alias GoogleApi.PubSub.V1.Connection

  alias GoogleApi.PubSub.V1.Model.{
    AcknowledgeRequest,
    ModifyAckDeadlineRequest,
    PubsubMessage,
    PullResponse,
    PullRequest,
    ReceivedMessage
  }

  alias Tesla.Adapter.Hackney
  require Logger

  @behaviour Client

  @default_max_number_of_messages 10

  @default_scope "https://www.googleapis.com/auth/pubsub"

  defp conn!(config, adapter_opts \\ []) do
    %{
      adapter: adapter,
      connection_pool: connection_pool,
      token_generator: {mod, fun, args}
    } = config

    {:ok, token} = apply(mod, fun, args)

    adapter_opts = Keyword.put(adapter_opts, :pool, connection_pool)

    token
    |> Connection.new()
    |> override_tesla_adapter({adapter, adapter_opts})
  end

  defp override_tesla_adapter(client, adapter) do
    %{adapter: adapter} = Tesla.client([], adapter)
    %{client | adapter: adapter}
  end

  @impl Client
  def prepare_to_connect(module, opts) do
    pool_name = Module.concat(module, ConnectionPool)

    pool_opts =
      opts
      |> Keyword.get(:pool_opts, [])
      |> Keyword.put_new_lazy(:max_connections, fn -> opts[:pool_size] end)

    pool_spec = :hackney_pool.child_spec(pool_name, pool_opts)

    {[pool_spec], Keyword.put(opts, :__connection_pool__, pool_name)}
  end

  @impl Client
  def init(opts) do
    with {:ok, subscription} <- validate_subscription(opts),
         {:ok, token_generator} <- validate_token_opts(opts),
         {:ok, pull_request} <- validate_pull_request(opts),
         {:ok, ack_config} <- ClientAcknowledger.init([client: __MODULE__] ++ opts) do
      adapter = Keyword.get(opts, :__internal_tesla_adapter__, Hackney)
      connection_pool = Keyword.get(opts, :__connection_pool__, :default)

      config = %{
        adapter: adapter,
        connection_pool: connection_pool,
        subscription: subscription,
        token_generator: token_generator
      }

      ack_ref = ClientAcknowledger.ack_ref(ack_config, config)

      {:ok, Map.merge(config, %{ack_ref: ack_ref, pull_request: pull_request})}
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
    |> handle_response(:receive_messages)
    |> wrap_received_messages(opts.ack_ref)
  end

  @impl Client
  def acknowledge(ack_ids, opts) do
    opts
    |> conn!()
    |> pubsub_projects_subscriptions_acknowledge(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: %AcknowledgeRequest{ackIds: ack_ids}
    )
    |> handle_response(:acknowledge)
  end

  @impl Client
  def put_deadline(ack_ids, deadline, opts) when deadline in 0..600 do
    opts
    |> conn!()
    |> pubsub_projects_subscriptions_modify_ack_deadline(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: %ModifyAckDeadlineRequest{ackDeadlineSeconds: deadline, ackIds: ack_ids}
    )
    |> handle_response(:put_deadline)
  end

  # The typespec for PullResponse is too strict. If Pub/Sub returns an empty
  # response, then `receivedMessages` will be nil.
  defp handle_response({:ok, %PullResponse{receivedMessages: nil}}, :receive_messages) do
    []
  end

  defp handle_response({:ok, response}, :receive_messages) do
    %PullResponse{receivedMessages: received_messages} = response
    received_messages
  end

  defp handle_response({:ok, _}, _), do: :ok

  defp handle_response({:error, reason}, :receive_messages) do
    Logger.error("Unable to fetch events from Cloud Pub/Sub. Reason: #{inspect(reason)}")
    []
  end

  defp handle_response({:error, reason}, :acknowledge) do
    Logger.error("Unable to acknowledge messages with Cloud Pub/Sub, reason: #{inspect(reason)}")
    :ok
  end

  defp handle_response({:error, reason}, :put_deadline) do
    Logger.error("Unable to put new ack deadline with Cloud Pub/Sub, reason: #{inspect(reason)}")
    :ok
  end

  defp wrap_received_messages(received_messages, ack_ref) do
    Enum.map(received_messages, fn received_message ->
      %ReceivedMessage{message: message, ackId: ack_id} = received_message

      {data, metadata} =
        message
        |> decode_message()
        |> Map.from_struct()
        |> Map.pop(:data)

      %Message{
        data: data,
        metadata: metadata,
        acknowledger: ClientAcknowledger.acknowledger(ack_id, ack_ref)
      }
    end)
  end

  defp decode_message(%PubsubMessage{data: nil} = message), do: message

  defp decode_message(%PubsubMessage{data: encoded_data} = message) do
    %{message | data: Base.decode64!(encoded_data)}
  end

  defp put_max_number_of_messages(pull_request, demand) do
    max_number_of_messages = min(demand, pull_request.maxMessages)

    %{pull_request | maxMessages: max_number_of_messages}
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
