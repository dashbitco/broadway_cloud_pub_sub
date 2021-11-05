defmodule BroadwayCloudPubSub.PullClient do
  @moduledoc """
  Pull client using Finch.
  """
  alias Broadway.Message
  alias BroadwayCloudPubSub.Client
  alias BroadwayCloudPubSub.PipelineOptions
  alias Finch.Response

  @behaviour Client

  @impl Client
  def prepare_to_connect(name, producer_opts) do
    # pool size is calculated in the BCPS Producer and guaranteed to be here
    pool_size = Keyword.fetch!(producer_opts, :pool_size)

    finch_name = Module.concat(name, PullClient)

    children = [
      {Finch, name: finch_name, pools: %{default: [size: pool_size]}}
    ]

    producer_opts = Keyword.put(producer_opts, :finch_name, finch_name)

    {children, producer_opts}
  end

  @impl Client
  def init(opts) do
    with {:ok, pipeline_config} <- PipelineOptions.validate(opts) do
      api_config = %{
        finch_name: opts[:finch_name],
        base_url: opts[:base_url]
      }

      finch_config = Map.merge(pipeline_config, api_config)

      {:ok, finch_config}
    end
  end

  @impl Client
  def receive_messages(demand, ack_builder, config) do
    max_messages = min(demand, config.pull_request.maxMessages)

    config
    |> execute(:pull, %{"maxMessages" => max_messages})
    |> Map.fetch!("receivedMessages")
    |> wrap_received_messages(ack_builder)
  end

  @impl Client
  def put_deadline(ack_ids, ack_deadline_seconds, config) do
    payload = %{
      "ackIds" => ack_ids,
      "ackDeadlineSeconds" => ack_deadline_seconds
    }

    execute(config, "modifyAckDeadline", payload)

    :ok
  end

  @impl Client
  def acknowledge(ack_ids, config) do
    execute(config, :acknowledge, %{"ackIds" => ack_ids})
  end

  defp wrap_received_messages(pub_sub_messages, ack_builder) do
    Enum.map(pub_sub_messages, fn pub_sub_msg ->
      pub_sub_msg_to_broadway_msg(pub_sub_msg, ack_builder)
    end)
  end

  defp pub_sub_msg_to_broadway_msg(pub_sub_msg, ack_builder) do
    %{
      "ackId" => ack_id,
      "message" => message
    } = pub_sub_msg

    {data, metadata} =
      message
      |> decode_message()
      |> Map.pop("data")

    metadata = %{
      attributes: metadata["attributes"],
      messageId: metadata["messageId"],
      publishTime: parse_datetime(metadata["publishTime"])
    }

    %Message{
      data: data,
      metadata: metadata,
      acknowledger: ack_builder.(ack_id)
    }
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _} ->
        dt

      err ->
        raise """
        invalid datetime string: #{inspect(err)}
        """
    end
  end

  defp decode_message(%{"data" => nil} = message), do: message

  defp decode_message(%{"data" => encoded_data} = message) do
    %{message | "data" => Base.decode64!(encoded_data)}
  end

  defp headers(config) do
    token = get_token(config)
    [{"authorization", "Bearer #{token}"}, {"content-type", "application/json"}]
  end

  defp url(config, action) do
    sub = URI.encode(config.subscription.string)
    path = "/v1/" <> sub <> ":" <> to_string(action)
    config.base_url <> path
  end

  defp status!(%Response{status: same} = resp, same) do
    resp
  end

  defp status!(%Response{status: got}, expected) do
    raise "#{inspect(__MODULE__)} - unexpected HTTP status code -" <>
            " expected: #{expected}, got: #{inspect(got)}"
  end

  defp json!(%Response{body: b}), do: Jason.decode!(b)

  defp execute(config, action, payload) do
    url = url(config, action)
    body = Jason.encode!(payload)
    token = get_token(config)
    headers = headers(token)

    config.finch_name
    |> finch_request!(url, body, headers)
    |> status!(200)
    |> json!()
  end

  defp finch_request!(finch_name, url, body, headers) do
    :post
    |> Finch.build(url, headers, body)
    |> Finch.request(finch_name, receive_timeout: :infinity)
    |> case do
      {:ok, resp} ->
        resp

      {:error, %{__exception__: true} = err} ->
        raise err
    end
  end

  defp get_token(%{token_generator: {m, f, a}}) do
    {:ok, token} = apply(m, f, a)
    token
  end
end
