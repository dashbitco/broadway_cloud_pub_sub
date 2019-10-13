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

    * `:on_success` - Optional. Configures the acking behaviour for successful messages.
       See the "Acking" section below for all the possible values. This option can also be changed for each
       message through `Broadway.Message.configure_ack/2`. Defaults to `:ack`.

    * `:on_failure` - Optional. Configures the acking messages for failed messages. See the "Acking" section
       below for all the possible values. This option can also be changed for each message through
       `Broadway.Message.configure_ack/2`. Defaults to `:ignore`

    * `:pool_opts` - Optional. A set of additional options to override the
       default `:hackney_pool` configuration options.

  ## Additional options

  These options applies to all producers, regardless of client implementation:

    * `:client` - Optional. A module that implements the `BroadwayCloudPubSub.Client`
      behaviour. This module is responsible for fetching and acknowledging the
      messages. Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs. Default
      is `BroadwayCloudPubSub.GoogleApiClient`.

    * `:pool_size` - Optional. The size of the connection pool. Default is
       twice the number of producer stages.

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

  ## Acking

  You can use the `:on_success` and `:on_failure` options to control how messages are acked on PubSub.
  By default successful messages are acked and failed messages are ignored.
  You can set `:on_success` and `:on_failure` when starting the PubSub producer,
  or change them for each message through `Broadway.Message.configure_ack/2`

  Here is the list of all possible values supported by `:on_success` and `:on_failure`:

  * `:ack` - Acknowledge the message. PubSub will mark the message as acked.
  * `:ignore` - Don't do anything. It won't notify to PubSub, and it will apply the default deadline.
  * `:nack` - Change the deadline to 0 seconds.
  * `{:nack, integer}` - Change the deadline to the seconds specified.


  Read more about modifying the deadline of the messages [in the PubSub documentation](https://cloud.google.com/pubsub/docs/reference/rest/v1/projects.subscriptions/modifyAckDeadline)
  """

  use GenStage

  @behaviour Broadway.Producer

  @default_client BroadwayCloudPubSub.GoogleApiClient
  @default_receive_interval 5000

  @impl Broadway.Producer
  def prepare_for_start(module, opts) do
    {me, my_opts} = opts[:producer][:module]
    client = Keyword.get(my_opts, :client, @default_client)

    my_opts =
      Keyword.put_new_lazy(my_opts, :pool_size, fn ->
        2 * opts[:producer][:stages]
      end)

    {specs, my_opts} = prepare_to_connect(module, client, my_opts)

    opts = put_in(opts, [:producer, :module], {me, my_opts})

    {specs, opts}
  end

  defp prepare_to_connect(module, client, producer_opts) do
    if Code.ensure_loaded?(client) and function_exported?(client, :prepare_to_connect, 2) do
      client.prepare_to_connect(module, producer_opts)
    else
      {[], producer_opts}
    end
  end

  @impl true
  def init(opts) do
    client = opts[:client] || @default_client
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
