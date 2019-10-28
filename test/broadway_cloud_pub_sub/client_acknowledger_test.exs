defmodule BroadwayCloudPubSub.ClientAcknowledgerTest do
  use ExUnit.Case
  alias BroadwayCloudPubSub.Client
  alias BroadwayCloudPubSub.ClientAcknowledger

  doctest ClientAcknowledger

  defmodule ClientWithOnlyAcknowledge do
    @behaviour Client

    @impl Client
    def init(opts), do: {:ok, opts}

    @impl Client
    def receive_messages(_demand, _opts), do: []

    @impl Client
    def acknowledge(_ack_ids, _opts), do: :ok
  end

  defmodule ClientWithOnlyPut do
    @behaviour Client

    @impl Client
    def init(opts), do: {:ok, opts}

    @impl Client
    def receive_messages(_demand, _opts), do: []

    @impl Client
    def put_deadline(_ack_ids, _deadline, _opts), do: :ok
  end

  defmodule ClientWithBroadwayAcknowledger do
    alias Broadway.Acknowledger

    @behaviour Client
    @behaviour Acknowledger

    @impl Client
    def init(opts), do: {:ok, opts}

    @impl Client
    def receive_messages(_demand, _opts), do: []

    @impl Acknowledger
    def ack(_ack_ref, _successful, _failed), do: :ok
  end

  defmodule CallerClient do
    alias BroadwayCloudPubSub.ClientAcknowledger

    @behaviour Client

    @impl Client
    def init(opts) do
      with {:ok, ack_config} <- ClientAcknowledger.init(opts) do
        config = %{test_pid: opts[:test_pid]}
        ack_ref = ClientAcknowledger.ack_ref(ack_config, config)

        {:ok, Map.put(config, :ack_ref, ack_ref)}
      end
    end

    @impl Client
    def receive_messages(_demand, _opts), do: []

    @impl Client
    def acknowledge(ack_ids, opts) do
      send(opts.test_pid, {:acknowledge, length(ack_ids)})
    end

    @impl Client
    def put_deadline(ack_ids, deadline, opts) do
      send(opts.test_pid, {{:put_deadline, deadline}, length(ack_ids)})
    end
  end

  describe "init/1" do
    test "when client is not an atom, returns error" do
      assert ClientAcknowledger.init(client: "a string") ==
               {:error,
                "expected :client to be a module implementing #{inspect(Client)}, got: \"a string\""}

      assert ClientAcknowledger.init(client: nil) ==
               {:error,
                "expected :client to be a module implementing #{inspect(Client)}, got: nil"}

      assert ClientAcknowledger.init(client: true) ==
               {:error,
                "expected :client to be a module implementing #{inspect(Client)}, got: true"}

      assert ClientAcknowledger.init(client: false) ==
               {:error,
                "expected :client to be a module implementing #{inspect(Client)}, got: nil"}
    end

    test "when client does not exist, returns error" do
      assert ClientAcknowledger.init(client: DoesNotExistClient) ==
               {:error, "the client DoesNotExistClient does not exist or could not be loaded"}
    end

    test "when client implements Broadway.Acknowledger, returns error" do
      assert ClientAcknowledger.init(client: ClientWithBroadwayAcknowledger) ==
               {:error,
                "the client #{inspect(ClientWithBroadwayAcknowledger)} is attempting to call #{
                  inspect(ClientAcknowledger)
                }.init/1, but the client itself implements the Broadway.Acknowledger behaviour"}
    end

    test "when client has only acknowledge/2, returns error" do
      assert ClientAcknowledger.init(client: ClientWithOnlyAcknowledge) ==
               {:error,
                "#{inspect(ClientWithOnlyAcknowledge)}.put_deadline/3 is undefined or private"}
    end

    test "when client has only put_deadline/3, returns error" do
      assert ClientAcknowledger.init(client: ClientWithOnlyPut) ==
               {:error, "#{inspect(ClientWithOnlyPut)}.acknowledge/2 is undefined or private"}
    end

    test "with valid client, returns config with default actions" do
      assert ClientAcknowledger.init(client: CallerClient) ==
               {:ok,
                %ClientAcknowledger{
                  client: CallerClient,
                  client_opts: nil,
                  on_failure: :ignore,
                  on_success: :ack
                }}
    end

    test "with valid options, returns config with custom actions" do
      assert ClientAcknowledger.init(client: CallerClient, on_success: :ignore, on_failure: :nack) ==
               {:ok,
                %ClientAcknowledger{
                  client: CallerClient,
                  client_opts: nil,
                  on_failure: {:nack, 0},
                  on_success: :ignore
                }}
    end
  end

  describe "configure/3" do
    test "raise on unsupported configure option" do
      assert_raise(ArgumentError, "unsupported configure option :on_other", fn ->
        ClientAcknowledger.configure(:ack_ref, %{}, on_other: :ack)
      end)
    end

    test "raise on unsupported on_success value" do
      error_msg = "expected :on_success to be a valid acknowledgement option, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        ClientAcknowledger.configure(:ack_ref, %{}, on_success: :unknown)
      end)
    end

    test "raise on unsupported on_failure value" do
      error_msg = "expected :on_failure to be a valid acknowledgement option, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        ClientAcknowledger.configure(:ack_ref, %{}, on_failure: :unknown)
      end)
    end

    test "set on_success correctly" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_success: :ack}

      assert {:ok, expected} == ClientAcknowledger.configure(:ack_ref, ack_data, on_success: :ack)
    end

    test "set on_success with ignore" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_success: :ignore}

      assert {:ok, expected} ==
               ClientAcknowledger.configure(:ack_ref, ack_data, on_success: :ignore)
    end

    test "set on_failure with deadline 0" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_failure: {:nack, 0}}

      assert {:ok, expected} ==
               ClientAcknowledger.configure(:ack_ref, ack_data, on_failure: :nack)
    end

    test "set on_failure with custom deadline" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_failure: {:nack, 60}}

      assert {:ok, expected} ==
               ClientAcknowledger.configure(:ack_ref, ack_data, on_failure: {:nack, 60})
    end
  end
end
