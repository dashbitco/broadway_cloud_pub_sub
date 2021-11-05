defmodule BroadwayCloudPubSub.GoogleApiClient do
  @moduledoc """
  Default Pub/Sub client used by `BroadwayCloudPubSub.Producer` to communicate with Google
  Cloud Pub/Sub service. This client implements the `BroadwayCloudPubSub.Client` behaviour
  which defines callbacks for receiving and acknowledging messages.
  """

  import GoogleApi.PubSub.V1.Api.Projects
  alias Broadway.Message
  alias BroadwayCloudPubSub.{Client, PipelineOptions}
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

  @retry_codes [408, 500, 502, 503, 504, 522, 524]
  @retry_opts [delay: 500, max_retries: 10]

  defp conn!(config, adapter_opts \\ []) do
    %{
      adapter: adapter,
      middleware: middleware,
      connection_pool: connection_pool,
      token_generator: {mod, fun, args}
    } = config

    {:ok, token} = apply(mod, fun, args)

    adapter_opts = Keyword.put(adapter_opts, :pool, connection_pool)

    token
    |> Connection.new()
    |> override_tesla_client({adapter, adapter_opts}, middleware)
  end

  defp override_tesla_client(client, adapter, middleware) do
    %{adapter: adapter, pre: pre} = Tesla.client(middleware, adapter)
    %{client | adapter: adapter, pre: client.pre ++ pre}
  end

  defp should_retry?({:ok, %{status: code}}), do: code in @retry_codes
  defp should_retry?({:error, _reason}), do: true
  defp should_retry?(_other), do: false

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
    with {:ok, pipeline_opts} <- PipelineOptions.validate(opts) do
      adapter = Keyword.get(opts, :__internal_tesla_adapter__, Hackney)
      connection_pool = Keyword.get(opts, :__connection_pool__, :default)

      retry_opts =
        [should_retry: &should_retry?/1]
        |> Keyword.merge(@retry_opts)
        |> Keyword.merge(Keyword.get(opts, :retry, []))

      middleware = [{Tesla.Middleware.Retry, retry_opts}] ++ Keyword.get(opts, :middleware, [])

      config =
        %{
          adapter: adapter,
          middleware: middleware,
          connection_pool: connection_pool
        }
        |> Map.merge(pipeline_opts)
        |> Map.update!(:pull_request, fn p -> struct!(PullRequest, p) end)

      {:ok, config}
    end
  end

  @impl Client
  def receive_messages(demand, ack_builder, opts) do
    pull_request = put_max_number_of_messages(opts.pull_request, demand)

    opts
    |> conn!(recv_timeout: :infinity)
    |> pubsub_projects_subscriptions_pull(
      opts.subscription.projects_id,
      opts.subscription.subscriptions_id,
      body: pull_request
    )
    |> handle_response(:receive_messages)
    |> wrap_received_messages(ack_builder)
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
    Logger.error("Unable to fetch events from Cloud Pub/Sub. Reason: #{inspect_error(reason)}")
    []
  end

  defp handle_response({:error, reason}, :acknowledge) do
    Logger.error(
      "Unable to acknowledge messages with Cloud Pub/Sub, reason: #{inspect_error(reason)}"
    )

    :ok
  end

  defp handle_response({:error, reason}, :put_deadline) do
    Logger.error(
      "Unable to put new ack deadline with Cloud Pub/Sub, reason: #{inspect_error(reason)}"
    )

    :ok
  end

  defp inspect_error(%Tesla.Env{} = env) do
    """
    \nRequest to #{inspect(env.url)} failed with status #{inspect(env.status)}, got:

    #{inspect(env.body)}
    """
  end

  defp inspect_error(reason) do
    inspect(reason)
  end

  defp wrap_received_messages(received_messages, ack_builder) do
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
        acknowledger: ack_builder.(ack_id)
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
end
