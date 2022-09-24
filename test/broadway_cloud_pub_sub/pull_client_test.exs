defmodule BroadwayCloudPubSub.PullClientTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Acknowledger
  alias BroadwayCloudPubSub.PullClient
  alias Broadway.Message

  @pull_response """
  {
    "receivedMessages": [
      {
        "ackId": "1",
        "deliveryAttempt": 1,
        "message": {
          "data": "TWVzc2FnZTE=",
          "messageId": "19917247034",
          "attributes": {
            "foo": "bar",
            "qux": ""
          },
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "2",
        "deliveryAttempt": 2,
        "message": {
          "data": "TWVzc2FnZTI=",
          "messageId": "19917247035",
          "attributes": {},
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "3",
        "deliveryAttempt": 3,
        "message": {
          "data": null,
          "messageId": "19917247036",
          "attributes": {
            "number": "three"
          },
          "publishTime": "2014-02-14T00:00:02Z"
        }
      },
      {
        "ackId": "4",
        "message": {
          "data": null,
          "messageId": "19917247037",
          "attributes": {},
          "publishTime": null
        }
      }
    ]
  }
  """

  @empty_response """
  {}
  """

  setup do
    server = Bypass.open()
    base_url = "http://localhost:#{server.port}"

    finch_name = __MODULE__.FinchName
    _ = start_supervised({Finch, name: finch_name})

    {:ok, server: server, base_url: base_url, finch_name: finch_name}
  end

  def on_pubsub_request(server, fun) when is_function(fun, 2) do
    test_pid = self()

    Bypass.expect(server, fn conn ->
      url = Plug.Conn.request_url(conn)
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      body = Jason.decode!(body)

      send(test_pid, {:http_request_called, %{url: url, body: body}})

      case fun.(url, body) do
        {:ok, resp_body} -> Plug.Conn.resp(conn, 200, resp_body)
        {:error, resp_body} -> Plug.Conn.resp(conn, 500, resp_body)
        {:error, status, resp_body} -> Plug.Conn.resp(conn, status, resp_body)
      end
    end)
  end

  defp init_with_ack_builder(opts) do
    # mimics workflow from Producer.prepare_for_start/2
    ack_ref = opts[:broadway][:name]
    fill_persistent_term(ack_ref, opts)

    {:ok, config} = PullClient.init(opts)
    {ack_ref, Acknowledger.builder(ack_ref), config}
  end

  describe "receive_messages/3" do
    setup %{server: server, base_url: base_url, finch_name: finch_name} do
      test_pid = self()

      on_pubsub_request(server, fn _url, _body ->
        {:ok, @pull_response}
      end)

      %{
        pid: test_pid,
        opts: [
          # will be injected by Broadway at runtime
          broadway: [name: :Broadway3],
          base_url: base_url,
          finch_name: finch_name,
          max_number_of_messages: 10,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          receive_timeout: :infinity
        ]
      }
    end

    test "returns a list of Broadway.Message with :data and :metadata set", %{
      opts: base_opts
    } do
      {:ok, opts} = PullClient.init(base_opts)
      [message1, message2, message3, message4] = PullClient.receive_messages(10, & &1, opts)

      assert %Message{data: "Message1", metadata: %{publishTime: %DateTime{}}} = message1

      assert message1.metadata.messageId == "19917247034"
      assert message1.metadata.deliveryAttempt == 1

      assert %{
               "foo" => "bar",
               "qux" => ""
             } = message1.metadata.attributes

      assert message2.data == "Message2"
      assert message2.metadata.messageId == "19917247035"
      assert message2.metadata.attributes == %{}
      assert message2.metadata.deliveryAttempt == 2

      assert %Message{data: nil} = message3
      assert message3.metadata.deliveryAttempt == 3

      assert %{
               "number" => "three"
             } = message3.metadata.attributes

      assert message4.metadata.publishTime == nil
      assert message4.metadata.deliveryAttempt == nil
    end

    test "returns an empty list when an empty response is returned by the server", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ ->
        {:ok, @empty_response}
      end)

      {:ok, opts} = PullClient.init(base_opts)

      assert [] == PullClient.receive_messages(10, & &1, opts)
    end

    test "if the request fails, returns an empty list and log the error", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ -> {:error, 403, @empty_response} end)

      {:ok, opts} = PullClient.init(base_opts)

      assert capture_log(fn ->
               assert PullClient.receive_messages(10, & &1, opts) == []
             end) =~ "[error] Unable to fetch events from Cloud Pub/Sub - reason: "
    end

    test "send a projects.subscriptions.pull request with default options", %{opts: base_opts} do
      {:ok, opts} = PullClient.init(base_opts)
      PullClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"maxMessages" => 10}
      assert url == base_opts[:base_url] <> "/v1/projects/foo/subscriptions/bar:pull"
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> PullClient.init()
      PullClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body["maxMessages"] == 5
    end
  end

  describe "acknowledge/2" do
    setup %{server: server, base_url: base_url, finch_name: finch_name} do
      test_pid = self()

      on_pubsub_request(server, fn _, _ ->
        {:ok, @empty_response}
      end)

      %{
        pid: test_pid,
        opts: [
          # will be injected by Broadway at runtime
          broadway: [name: :Broadway3],
          base_url: base_url,
          finch_name: finch_name,
          max_number_of_messages: 10,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          receive_timeout: :infinity
        ]
      }
    end

    test "makes a projects.subscriptions.acknowledge request", %{opts: base_opts} do
      {:ok, opts} = PullClient.init(base_opts)

      PullClient.acknowledge(["1", "2", "3"], opts)

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body == %{"ackIds" => ["1", "2", "3"]}
      base_url = base_opts[:base_url]
      assert url == base_url <> "/v1/projects/foo/subscriptions/bar:acknowledge"
    end

    test "if the request fails, returns :ok and logs an error", %{
      opts: base_opts,
      server: server
    } do
      on_pubsub_request(server, fn _, _ ->
        {:error, 503, @empty_response}
      end)

      {:ok, opts} = PullClient.init(base_opts)

      assert capture_log(fn ->
               assert PullClient.acknowledge(["1", "2"], opts) == :ok
             end) =~ "[error] Unable to acknowledge messages with Cloud Pub/Sub - reason: "
    end
  end

  describe "put_deadline/3" do
    setup %{server: server, base_url: base_url, finch_name: finch_name} do
      test_pid = self()

      on_pubsub_request(server, fn _, _ ->
        {:ok, @empty_response}
      end)

      %{
        pid: test_pid,
        opts: [
          # will be injected by Broadway at runtime
          broadway: [name: :Broadway3],
          base_url: base_url,
          finch_name: finch_name,
          max_number_of_messages: 10,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          receive_timeout: :infinity
        ]
      }
    end

    test "makes a projects.subscriptions.modifyAckDeadline request", %{
      opts: base_opts
    } do
      {:ok, opts} = PullClient.init(base_opts)

      ack_ids = ["1", "2"]
      PullClient.put_deadline(ack_ids, 30, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"ackIds" => ack_ids, "ackDeadlineSeconds" => 30}

      assert url == base_opts[:base_url] <> "/v1/projects/foo/subscriptions/bar:modifyAckDeadline"
    end

    test "if the request fails, returns :ok and logs an error",
         %{opts: base_opts, server: server} do
      on_pubsub_request(server, fn _, _ ->
        {:error, 503, @empty_response}
      end)

      {:ok, opts} = PullClient.init(base_opts)

      assert capture_log(fn ->
               assert PullClient.put_deadline(["1", "2"], 60, opts) == :ok
             end) =~ "[error] Unable to put new ack deadline with Cloud Pub/Sub - reason: "
    end
  end

  describe "prepare_to_connect/2" do
    test "returns a child_spec for starting a Finch http pool " do
      {[pool_spec], opts} = PullClient.prepare_to_connect(SomePipeline, pool_size: 2)
      assert pool_spec == {Finch, name: SomePipeline.PullClient, pools: %{default: [size: 2]}}
      assert opts == [finch_name: SomePipeline.PullClient, pool_size: 2]
    end

    test "allows custom finch_name" do
      {[pool_spec], opts} =
        PullClient.prepare_to_connect(SomePipeline, finch_name: Foo, pool_size: 2)

      assert pool_spec == {Finch, name: Foo, pools: %{default: [size: 2]}}
      assert opts == [finch_name: Foo, pool_size: 2]
    end
  end

  describe "integration with BroadwayCloudPubSub.Acknowledger" do
    setup %{server: server, base_url: base_url, finch_name: finch_name} do
      test_pid = self()

      on_pubsub_request(server, fn url, body ->
        action =
          url
          |> String.split(":")
          |> List.last()

        case action do
          "pull" ->
            send(test_pid, {:pull_dispatched, %{url: url, body: body}})
            {:ok, @pull_response}

          "acknowledge" ->
            %{"ackIds" => ack_ids} = body

            send(test_pid, {:acknowledge_dispatched, length(ack_ids), ack_ids})
            {:ok, @empty_response}

          "modifyAckDeadline" ->
            %{"ackIds" => ack_ids, "ackDeadlineSeconds" => deadline} = body

            send(
              test_pid,
              {:modack_dispatched, length(ack_ids), deadline}
            )

            {:ok, @empty_response}
        end
      end)

      {:ok,
       %{
         pid: test_pid,
         opts: [
           # will be injected by Broadway at runtime
           broadway: [name: :Broadway3],
           base_url: base_url,
           client: PullClient,
           finch_name: finch_name,
           max_number_of_messages: 10,
           subscription: "projects/foo/subscriptions/bar",
           token_generator: {__MODULE__, :generate_token, []},
           receive_timeout: :infinity
         ]
       }}
    end

    test "returns a list of Broadway.Message structs with ack builder", %{
      opts: base_opts
    } do
      {:ok, opts} = PullClient.init(base_opts)

      [message1, message2, message3, message4] =
        PullClient.receive_messages(10, &{:ack, &1}, opts)

      assert {:ack, _} = message1.acknowledger
      assert {:ack, _} = message2.acknowledger
      assert {:ack, _} = message3.acknowledger
      assert {:ack, _} = message4.acknowledger
    end

    test "with defaults successful messages are acknowledged, and failed messages are ignored", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} = init_with_ack_builder(base_opts)

      messages = PullClient.receive_messages(10, builder, opts)

      {successful, failed} = Enum.split(messages, 1)

      Acknowledger.ack(ack_ref, successful, failed)

      assert_receive {:acknowledge_dispatched, 1, ["1"]}
    end

    test "when :on_success is :noop, acknowledgement is a no-op", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, :noop)
        |> init_with_ack_builder()

      [_, _, _, _] = messages = PullClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "when :on_success is :nack, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, :nack)
        |> init_with_ack_builder()

      [_, _, _, _] = messages = PullClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      assert_receive {:modack_dispatched, 4, 0}
      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "when :on_success is {:nack, integer}, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, {:nack, 300})
        |> init_with_ack_builder()

      [_, _, _, _] = messages = PullClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      assert_receive {:modack_dispatched, 4, 300}
      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "with default :on_failure, failed messages are ignored", %{opts: base_opts} do
      {ack_ref, builder, opts} = init_with_ack_builder(base_opts)

      [_, _, _, _] = messages = PullClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      refute_receive {:acknowledge_dispatched, 4, _}
    end

    test "when :on_failure is :nack, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_failure, :nack)
        |> init_with_ack_builder()

      [_, _, _, _] = messages = PullClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      assert_receive {:modack_dispatched, 4, 0}
    end

    test "when :on_failure is {:nack, integer}, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_failure, {:nack, 60})
        |> init_with_ack_builder()

      [_, _, _, _] = messages = PullClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      assert_receive {:modack_dispatched, 4, 60}
    end
  end

  def generate_token, do: {:ok, "token.#{System.os_time(:second)}"}

  defp fill_persistent_term(ack_ref, base_opts) do
    :persistent_term.put(ack_ref, %{
      base_url: Keyword.fetch!(base_opts, :base_url),
      client: PullClient,
      finch_name: Keyword.fetch!(base_opts, :finch_name),
      on_failure: base_opts[:on_failure] || :noop,
      on_success: base_opts[:on_success] || :ack,
      subscription: "projects/test/subscriptions/test-subscription",
      token_generator: {__MODULE__, :generate_token, []},
      receive_timeout: base_opts[:receive_timeout] || :infinity
    })
  end
end
