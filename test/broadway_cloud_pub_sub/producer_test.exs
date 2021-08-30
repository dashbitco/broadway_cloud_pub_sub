defmodule BroadwayCloudPubSub.ProducerTest do
  use ExUnit.Case

  alias Broadway.Message

  defmodule MessageServer do
    def start_link() do
      Agent.start_link(fn -> [] end)
    end

    def push_messages(server, messages) do
      Agent.update(server, fn queue -> queue ++ messages end)
    end

    def take_messages(server, amount) do
      Agent.get_and_update(server, &Enum.split(&1, amount))
    end
  end

  defmodule FakeClient do
    alias BroadwayCloudPubSub.Client
    alias Broadway.Acknowledger

    @behaviour Client
    @behaviour Acknowledger

    @impl Client
    def init(opts), do: {:ok, opts}

    @impl Client
    def receive_messages(amount, _builder, opts) do
      messages = MessageServer.take_messages(opts[:message_server], amount)
      send(opts[:test_pid], {:messages_received, length(messages)})

      for msg <- messages do
        ack_data = %{
          receipt: %{id: "Id_#{msg}", receipt_handle: "ReceiptHandle_#{msg}"},
          test_pid: opts[:test_pid]
        }

        %Message{data: msg, acknowledger: {__MODULE__, :ack_ref, ack_data}}
      end
    end

    @impl Acknowledger
    def ack(_ack_ref, successful, _failed) do
      [%Message{acknowledger: {_, _, %{test_pid: test_pid}}} | _] = successful
      send(test_pid, {:messages_deleted, length(successful)})
    end
  end

  defmodule FakePool do
    use GenServer

    def start_link(opts) do
      GenServer.start_link(__MODULE__, opts, name: opts[:name])
    end

    def init(opts) do
      send(opts[:test_pid], {:pool_started, opts[:name]})

      {:ok, opts}
    end

    def child_spec(name, opts) do
      {__MODULE__, Keyword.put(opts, :name, name)}
    end

    def pool_size(pool), do: GenServer.call(pool, :pool_size)

    def handle_call(:pool_size, _, opts) do
      {:reply, opts[:pool_size], opts}
    end
  end

  defmodule FakePoolClient do
    alias BroadwayCloudPubSub.Client

    @behaviour Client

    @impl Client
    def prepare_to_connect(module, opts) do
      pool = Module.concat(module, FakePool)
      pool_spec = FakePool.child_spec(pool, opts)

      {[pool_spec], Keyword.put(opts, :__connection_pool__, pool)}
    end

    @impl Client
    def init(opts) do
      send(opts[:test_pid], {:connection_pool_set, opts[:__connection_pool__]})

      {:ok, opts}
    end

    @impl Client
    def receive_messages(_amount, _builder, _opts), do: []
  end

  defmodule Forwarder do
    use Broadway

    def handle_message(_, message, %{test_pid: test_pid}) do
      send(test_pid, {:message_handled, message.data})
      message
    end

    def handle_batch(_, messages, _, _) do
      messages
    end
  end

  test "raise an ArgumentError with proper message when client options are invalid" do
    assert_raise(
      ArgumentError,
      "expected :subscription to be a non empty string, got: nil",
      fn ->
        BroadwayCloudPubSub.Producer.init(subscription: nil)
      end
    )
  end

  test "receive messages when the queue has less than the demand" do
    {:ok, message_server} = MessageServer.start_link()
    name = start_broadway(message_server)

    MessageServer.push_messages(message_server, 1..5)

    assert_receive {:messages_received, 5}

    for msg <- 1..5 do
      assert_receive {:message_handled, ^msg}
    end

    stop_broadway(name)
  end

  test "keep receiving messages when the queue has more than the demand" do
    {:ok, message_server} = MessageServer.start_link()
    MessageServer.push_messages(message_server, 1..20)
    name = start_broadway(message_server)

    assert_receive {:messages_received, 10}

    for msg <- 1..10 do
      assert_receive {:message_handled, ^msg}
    end

    assert_receive {:messages_received, 5}

    for msg <- 11..15 do
      assert_receive {:message_handled, ^msg}
    end

    assert_receive {:messages_received, 5}

    for msg <- 16..20 do
      assert_receive {:message_handled, ^msg}
    end

    assert_receive {:messages_received, 0}

    stop_broadway(name)
  end

  test "keep trying to receive new messages when the queue is empty" do
    {:ok, message_server} = MessageServer.start_link()
    name = start_broadway(message_server)

    MessageServer.push_messages(message_server, [13])
    assert_receive {:messages_received, 1}
    assert_receive {:message_handled, 13}

    assert_receive {:messages_received, 0}
    refute_receive {:message_handled, _}

    MessageServer.push_messages(message_server, [14, 15])
    assert_receive {:messages_received, 2}
    assert_receive {:message_handled, 14}
    assert_receive {:message_handled, 15}

    stop_broadway(name)
  end

  test "stop trying to receive new messages after start draining" do
    {:ok, message_server} = MessageServer.start_link()
    name = start_broadway(message_server)

    [producer] = Broadway.producer_names(name)

    assert_receive {:messages_received, 0}

    :sys.suspend(producer)
    flush_messages_received()
    task = Task.async(fn -> Broadway.Topology.ProducerStage.drain(producer) end)
    :sys.resume(producer)
    Task.await(task)

    refute_receive {:messages_received, _}, 10

    stop_broadway(name)
  end

  test "delete acknowledged messages" do
    {:ok, message_server} = MessageServer.start_link()
    name = start_broadway(message_server)

    MessageServer.push_messages(message_server, 1..20)

    assert_receive {:messages_deleted, 10}
    assert_receive {:messages_deleted, 10}

    stop_broadway(name)
  end

  describe "calling Client.prepare_to_connect/2" do
    test "with default options, pool_size is twice the producers" do
      {:ok, message_server} = MessageServer.start_link()
      name = start_broadway(message_server, FakePoolClient)

      assert_receive {:pool_started, pool}, 500
      assert_receive {:connection_pool_set, ^pool}, 500
      assert FakePool.pool_size(pool) == 2

      stop_broadway(name)
    end

    test "with user-defined pool_size" do
      {:ok, message_server} = MessageServer.start_link()
      name = start_broadway(message_server, FakePoolClient, pool_size: 20)

      assert_receive {:pool_started, pool}, 500
      assert_receive {:connection_pool_set, ^pool}, 500
      assert FakePool.pool_size(pool) == 20

      stop_broadway(name)
    end
  end

  defp start_broadway(message_server, client \\ FakeClient, extra_opts \\ []) do
    producer_opts =
      Keyword.merge(extra_opts,
        client: client,
        receive_interval: 0,
        test_pid: self(),
        message_server: message_server
      )

    name = new_unique_name()

    {:ok, _pid} =
      Broadway.start_link(
        Forwarder,
        name: name,
        context: %{test_pid: self()},
        producer: [
          module: {
            BroadwayCloudPubSub.Producer,
            producer_opts
          },
          concurrency: 1
        ],
        processors: [
          default: [concurrency: 1]
        ],
        batchers: [
          default: [
            batch_size: 10,
            batch_timeout: 50,
            concurrency: 1
          ]
        ]
      )

    name
  end

  defp new_unique_name() do
    :"Broadway#{System.unique_integer([:positive, :monotonic])}"
  end

  defp stop_broadway(name) when is_atom(name) do
    pid = Process.whereis(name)
    ref = Process.monitor(pid)
    Process.exit(pid, :normal)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end

  defp flush_messages_received() do
    receive do
      {:messages_received, 0} -> flush_messages_received()
    after
      0 -> :ok
    end
  end
end
