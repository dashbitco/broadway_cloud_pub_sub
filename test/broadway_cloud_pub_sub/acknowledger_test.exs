defmodule BroadwayCloudPubSub.AcknowledgerTest do
  use ExUnit.Case
  alias Broadway.Message
  alias BroadwayCloudPubSub.Client
  alias BroadwayCloudPubSub.Acknowledger

  doctest Acknowledger

  defmodule CallerClient do
    alias BroadwayCloudPubSub.Acknowledger

    @behaviour Client

    @impl Client
    def init(opts) do
      {:ok, %{test_pid: opts[:test_pid]}}
    end

    @impl Client
    def receive_messages(_demand, _ack_builder, _opts), do: []

    @impl Client
    def acknowledge(ack_ids, config) do
      send(config.test_pid, {:acknowledge, length(ack_ids)})
    end

    @impl Client
    def put_deadline(ack_ids, deadline, config) do
      send(config.test_pid, {:put_deadline, length(ack_ids), deadline})
    end
  end

  defp init_with_ack_ref(opts) do
    {:ok, config} = CallerClient.init(opts)
    {:ok, ack_ref} = Acknowledger.init(CallerClient, config, opts)
    ack_ref
  end

  describe "init/1" do
    test "with valid client, returns config with default actions" do
      assert {:ok, ref} = Acknowledger.init(CallerClient, :config, [])

      assert Acknowledger.get_config(ref) ==
               %Acknowledger{
                 client: CallerClient,
                 client_config: :config,
                 on_failure: :noop,
                 on_success: :ack
               }
    end

    test "with valid options, returns config with custom actions" do
      assert {:ok, ref} =
               Acknowledger.init(CallerClient, :config, on_success: :noop, on_failure: :nack)

      assert Acknowledger.get_config(ref) ==
               %Acknowledger{
                 client: CallerClient,
                 client_config: :config,
                 on_failure: {:nack, 0},
                 on_success: :noop
               }
    end
  end

  describe "configure/3" do
    test "raise on unsupported configure option" do
      assert_raise(ArgumentError, "unsupported configure option :on_other", fn ->
        Acknowledger.configure(:ack_ref, %{}, on_other: :ack)
      end)
    end

    test "raise on unsupported on_success value" do
      error_msg = "expected :on_success to be a valid acknowledgement option, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        Acknowledger.configure(:ack_ref, %{}, on_success: :unknown)
      end)
    end

    test "raise on unsupported on_failure value" do
      error_msg = "expected :on_failure to be a valid acknowledgement option, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        Acknowledger.configure(:ack_ref, %{}, on_failure: :unknown)
      end)
    end

    test "set on_success correctly" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_success: :ack}

      assert {:ok, expected} == Acknowledger.configure(:ack_ref, ack_data, on_success: :ack)
    end

    test "set on_success with ignore" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_success: :noop}

      assert {:ok, expected} ==
               Acknowledger.configure(:ack_ref, ack_data, on_success: :noop)
    end

    test "set on_failure with deadline 0" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_failure: {:nack, 0}}

      assert {:ok, expected} ==
               Acknowledger.configure(:ack_ref, ack_data, on_failure: :nack)
    end

    test "set on_failure with custom deadline" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_failure: {:nack, 60}}

      assert {:ok, expected} ==
               Acknowledger.configure(:ack_ref, ack_data, on_failure: {:nack, 60})
    end
  end

  describe "ack/3" do
    setup do
      producer_opts = [client: CallerClient, test_pid: self()]

      {:ok, producer_opts: producer_opts}
    end

    test "with defaults, only successful messages are acknowledged", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref(opts)

      messages = build_messages(6, ack_ref)

      {successful, failed} = Enum.split(messages, 3)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_received {:acknowledge, 3}
    end

    test "overriding default on_success", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref([on_success: :noop] ++ opts)

      messages = build_messages(6, ack_ref)

      {successful, failed} = Enum.split(messages, 3)

      Acknowledger.ack(ack_ref, successful, failed)

      refute_received {:acknowledge, 3}
    end

    test "overriding default on_failure", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref([on_failure: :ack] ++ opts)

      messages = build_messages(6, ack_ref)

      {successful, failed} = Enum.split(messages, 3)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_received {:acknowledge, 6}
    end

    test "overriding message on_success", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref(opts)

      [first | rest] = build_messages(3, ack_ref)

      first = Message.configure_ack(first, on_success: :nack)

      Acknowledger.ack(ack_ref, [first | rest], [])

      assert_received({:acknowledge, 2})
      assert_received({:put_deadline, 1, 0})
    end

    test "overriding message on_failure", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref(opts)

      [first | rest] = build_messages(3, ack_ref)

      first = Message.configure_ack(first, on_failure: :nack)

      Acknowledger.ack(ack_ref, rest, [first])

      assert_received({:acknowledge, 2})
      assert_received({:put_deadline, 1, 0})
    end

    test "groups successful and failed messages by action", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref([on_failure: :ack] ++ opts)

      messages = build_messages(6, ack_ref)

      {successful, failed} = Enum.split(messages, 3)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_received({:acknowledge, 6})
    end

    test "treats :nack as {:nack, 0}", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref([on_success: :nack, on_failure: {:nack, 0}] ++ opts)

      messages = build_messages(6, ack_ref)

      {successful, failed} = Enum.split(messages, 3)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_received({:put_deadline, 6, 0})
    end

    test "configuring message treats :nack as {:nack, 0}", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref([on_success: :nack, on_failure: {:nack, 0}] ++ opts)

      [first | messages] = build_messages(6, ack_ref)

      first = Message.configure_ack(first, on_success: :nack)

      {successful, failed} = Enum.split([first | messages], 3)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_received({:put_deadline, 6, 0})
    end

    test "chunks actions every 3_000 ack_ids", %{producer_opts: opts} do
      ack_ref = init_with_ack_ref([on_failure: :nack] ++ opts)

      messages = build_messages(10_000, ack_ref)

      {successful, failed} = Enum.split(messages, 3_500)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_received({:acknowledge, 2_500})
      assert_received({:acknowledge, 1_000})
      assert_received({:put_deadline, 2_500, 0})
      assert_received({:put_deadline, 2_500, 0})
      assert_received({:put_deadline, 1_500, 0})
    end
  end

  defp build_messages(n, ack_ref) when is_integer(n) and n > 1 do
    Enum.map(1..n, &build_message(&1, ack_ref))
  end

  defp build_message(data, ack_ref) do
    acknowledger = Acknowledger.builder(ack_ref).("Ack_#{inspect(data)}")
    %Message{data: "Message_#{inspect(data)}", acknowledger: acknowledger}
  end
end
