defmodule BroadwayCloudPubSub.Producer do
  @moduledoc """
  A GenStage producer that continuously receives messages from a Google Cloud Pub/Sub
  topic and acknowledges them after being successfully processed.

  By default this producer uses `BroadwayCloudPubSub.GoogleApiClient` to talk to Cloud
  Pub/Sub, but you can provide your client by implementing the `BroadwayCloudPubSub.Client`
  behaviour.

  ## Options using `BroadwayCloudPubSub.GoogleApiClient`

    * `:subscription` - Required. The name of the subscription.
      Example: "projects/my-project/subscriptions/my-subscription"

    * `:max_number_of_messages` - Optional. The maximum number of messages to be fetched
      per request. Default is `10`.

    * `:return_immediately` - Optional. If this field set to true, the system will respond immediately
      even if it there are no messages available to return in the Pull response. Otherwise, the system
      may wait (for a bounded amount of time) until at least one message is available, rather than
      returning no messages. Default is `nil`.

    * `:scope` - Optional. A string representing the scope or scopes to use when fetching
       an access token. Default is `"https://www.googleapis.com/auth/pubsub"`.
       Note: The `:scope` option only applies to the default token generator.

    * `:token_generator` - Optional. An MFArgs tuple that will be called before each request
      to fetch an authentication token. It should return `{:ok, String.t()} | {:error, any()}`.
      Default generator uses `Goth.Token.for_scope/1` with `"https://www.googleapis.com/auth/pubsub"`.

  ## Additional options

  These options applies to all producers, regardless of client implementation:

    * `:client` - Optional. A module that implements the `BroadwayCloudPubSub.Client`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `BroadwayCloudPubSub.GoogleApiClient`.

    * `:receive_interval` - Optional. The duration (in milliseconds) for which the producer
      waits before making a request for more messages. Default is 5000.

  ### Example

      Broadway.start_link(MyBroadway,
        name: MyBroadway,
        producers: [
          default: [
            module: {BroadwayCloudPubSub.Producer,
              subscription: "projects/my-project/subscriptions/my_subscription"
            }
          ]
        ]
      )

  The above configuration will set up a producer that continuously receives messages
  from `"projects/my-project/subscriptions/my_subscription"` and sends them downstream.
  """

  use GenStage

  @default_receive_interval 5000

  @impl true
  def init(opts) do
    client = opts[:client] || BroadwayCloudPubSub.GoogleApiClient
    receive_interval = opts[:receive_interval] || @default_receive_interval

    case client.init(opts) do
      {:error, message} ->
        raise ArgumentError, "invalid options given to #{inspect(client)}.init/1, " <> message

      {:ok, opts} ->
        {:producer,
         %{
           demand: 0,
           receive_timer: nil,
           receive_interval: receive_interval,
           client: {client, opts}
         }}
    end
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_receive_messages(%{state | demand: demand + incoming_demand})
  end

  @impl true
  def handle_info(:receive_messages, state) do
    handle_receive_messages(%{state | receive_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp handle_receive_messages(%{receive_timer: nil, demand: demand} = state) when demand > 0 do
    messages = receive_messages_from_pubsub(state, demand)
    new_demand = demand - length(messages)

    receive_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_receive_messages(state.receive_interval)
        {_, 0} -> nil
        _ -> schedule_receive_messages(0)
      end

    {:noreply, messages, %{state | demand: new_demand, receive_timer: receive_timer}}
  end

  defp handle_receive_messages(state) do
    {:noreply, [], state}
  end

  defp receive_messages_from_pubsub(state, total_demand) do
    %{client: {client, opts}} = state
    client.receive_messages(total_demand, opts)
  end

  defp schedule_receive_messages(interval) do
    Process.send_after(self(), :receive_messages, interval)
  end
end
