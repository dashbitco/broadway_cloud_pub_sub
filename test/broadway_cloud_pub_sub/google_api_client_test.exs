defmodule BroadwayCloudPubSub.GoogleApiClientTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.Acknowledger
  alias BroadwayCloudPubSub.GoogleApiClient
  alias Broadway.Message

  @subscription_base "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar"

  @pull_response """
  {
    "receivedMessages": [
      {
        "ackId": "1",
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
        "message": {
          "data": "TWVzc2FnZTI=",
          "messageId": "19917247035",
          "attributes": {},
          "publishTime": "2014-02-14T00:00:01Z"
        }
      },
      {
        "ackId": "3",
        "message": {
          "data": null,
          "messageId": "19917247036",
          "attributes": {
            "number": "three"
          },
          "publishTime": "2014-02-14T00:00:02Z"
        }
      }
    ]
  }
  """

  @empty_response """
  {}
  """

  defp init_with_ack_builder(opts) do
    {:ok, config} = GoogleApiClient.init(opts)
    {:ok, ack_ref} = Acknowledger.init(GoogleApiClient, config, opts)
    {ack_ref, Acknowledger.builder(ack_ref), config}
  end

  describe "validate init options" do
    test ":subscription is required" do
      assert GoogleApiClient.init([]) ==
               {:error, "expected :subscription to be a non empty string, got: nil"}

      assert GoogleApiClient.init(subscription: nil) ==
               {:error, "expected :subscription to be a non empty string, got: nil"}
    end

    test ":subscription should be a valid subscription name" do
      assert GoogleApiClient.init(subscription: "") ==
               {:error, "expected :subscription to be a non empty string, got: \"\""}

      assert GoogleApiClient.init(subscription: :an_atom) ==
               {:error, "expected :subscription to be a non empty string, got: :an_atom"}

      assert {:ok, %{subscription: subscription}} =
               GoogleApiClient.init(subscription: "projects/foo/subscriptions/bar")

      assert subscription.projects_id == "foo"
      assert subscription.subscriptions_id == "bar"
    end

    test ":return_immediately is nil without default value" do
      {:ok, result} = GoogleApiClient.init(subscription: "projects/foo/subscriptions/bar")

      assert is_nil(result.pull_request.returnImmediately)
    end

    test ":return immediately should be a boolean" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:return_immediately, true) |> GoogleApiClient.init()
      assert result.pull_request.returnImmediately == true

      {:ok, result} = opts |> Keyword.put(:return_immediately, false) |> GoogleApiClient.init()
      assert is_nil(result.pull_request.returnImmediately)

      {:error, message} =
        opts |> Keyword.put(:return_immediately, "true") |> GoogleApiClient.init()

      assert message == "expected :return_immediately to be a boolean value, got: \"true\""

      {:error, message} = opts |> Keyword.put(:return_immediately, 0) |> GoogleApiClient.init()

      assert message == "expected :return_immediately to be a boolean value, got: 0"

      {:error, message} =
        opts |> Keyword.put(:return_immediately, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :return_immediately to be a boolean value, got: :an_atom"
    end

    test ":max_number_of_messages is optional with default value 10" do
      {:ok, result} = GoogleApiClient.init(subscription: "projects/foo/subscriptions/bar")

      assert result.pull_request.maxMessages == 10
    end

    test ":max_number_of_messages should be a positive integer" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:max_number_of_messages, 1) |> GoogleApiClient.init()
      assert result.pull_request.maxMessages == 1

      {:ok, result} = opts |> Keyword.put(:max_number_of_messages, 10) |> GoogleApiClient.init()
      assert result.pull_request.maxMessages == 10

      {:error, message} =
        opts |> Keyword.put(:max_number_of_messages, 0) |> GoogleApiClient.init()

      assert message == "expected :max_number_of_messages to be a positive integer, got: 0"

      {:error, message} =
        opts |> Keyword.put(:max_number_of_messages, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :max_number_of_messages to be a positive integer, got: :an_atom"
    end

    test ":scope should be a string or tuple" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:scope, "https://example.com") |> GoogleApiClient.init()

      assert {_, _, ["https://example.com"]} = result.token_generator

      {:error, message} = opts |> Keyword.put(:scope, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string or tuple, got: :an_atom"

      {:error, message} = opts |> Keyword.put(:scope, 1) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string or tuple, got: 1"

      {:error, message} = opts |> Keyword.put(:scope, {}) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string or tuple, got: {}"

      {:ok, result} =
        opts
        |> Keyword.put(:scope, {"mail@example.com", "https://example.com"})
        |> GoogleApiClient.init()

      assert {_, _, [{"mail@example.com", "https://example.com"}]} = result.token_generator
    end

    test ":token_generator defaults to using Goth with default scope" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = GoogleApiClient.init(opts)

      assert result.token_generator ==
               {BroadwayCloudPubSub.GoogleApiClient, :generate_goth_token,
                ["https://www.googleapis.com/auth/pubsub"]}
    end

    test ":token_generator should be a tuple {Mod, Fun, Args}" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      token_generator = {Token, :fetch, []}

      {:ok, result} =
        opts
        |> Keyword.put(:token_generator, token_generator)
        |> GoogleApiClient.init()

      assert result.token_generator == token_generator

      {:error, message} =
        opts
        |> Keyword.put(:token_generator, {1, 1, 1})
        |> GoogleApiClient.init()

      assert message == "expected :token_generator to be a tuple {Mod, Fun, Args}, got: {1, 1, 1}"

      {:error, message} =
        opts
        |> Keyword.put(:token_generator, SomeModule)
        |> GoogleApiClient.init()

      assert message ==
               "expected :token_generator to be a tuple {Mod, Fun, Args}, got: SomeModule"
    end

    test ":token_generator supercedes :scope validation" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      assert {:ok, _result} =
               opts
               |> Keyword.put(:scope, :an_invalid_scope)
               |> Keyword.put(:token_generator, {__MODULE__, :generate_token, []})
               |> GoogleApiClient.init()
    end
  end

  describe "receive_messages/3" do
    setup do
      test_pid = self()

      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(test_pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 200, body: @pull_response}
      end)

      %{
        pid: test_pid,
        opts: [
          __internal_tesla_adapter__: Tesla.Mock,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          retry: [max_retries: 3, delay: 1]
        ]
      }
    end

    test "returns a list of Broadway.Message with :data and :metadata set", %{
      opts: base_opts
    } do
      {:ok, opts} = GoogleApiClient.init(base_opts)
      [message1, message2, message3] = GoogleApiClient.receive_messages(10, & &1, opts)

      assert %Message{data: "Message1", metadata: %{publishTime: %DateTime{}}} = message1

      assert message1.metadata.messageId == "19917247034"

      assert %{
               "foo" => "bar",
               "qux" => ""
             } = message1.metadata.attributes

      assert message2.data == "Message2"
      assert message2.metadata.messageId == "19917247035"
      assert message2.metadata.attributes == %{}

      assert %Message{data: nil} = message3

      assert %{
               "number" => "three"
             } = message3.metadata.attributes
    end

    test "if the request fails, returns an empty list and log the error", %{
      pid: pid,
      opts: base_opts
    } do
      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 403, body: %{}}
      end)

      {:ok, opts} = GoogleApiClient.init(base_opts)

      assert capture_log(fn ->
               assert GoogleApiClient.receive_messages(10, & &1, opts) == []
             end) =~ "[error] Unable to fetch events from Cloud Pub/Sub. Reason: "
    end

    test "send a projects.subscriptions.pull request with default options", %{opts: base_opts} do
      {:ok, opts} = GoogleApiClient.init(base_opts)
      GoogleApiClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"maxMessages" => 10}
      assert url == "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:pull"
    end

    test "request with custom :return_immediately", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:return_immediately, true) |> GoogleApiClient.init()
      GoogleApiClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body["returnImmediately"] == true
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> GoogleApiClient.init()
      GoogleApiClient.receive_messages(10, & &1, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body["maxMessages"] == 5
    end
  end

  describe "acknowledge/2" do
    setup do
      test_pid = self()

      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(test_pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 200, body: @empty_response}
      end)

      %{
        pid: test_pid,
        opts: [
          __internal_tesla_adapter__: Tesla.Mock,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          retry: [max_retries: 3, delay: 1]
        ]
      }
    end

    test "makes a projects.subscriptions.acknowledge request", %{opts: base_opts} do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      GoogleApiClient.acknowledge(["1", "2", "3"], opts)

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body == %{"ackIds" => ["1", "2", "3"]}
      assert url == "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:acknowledge"
    end

    test "retries a few times in case of error response", %{opts: base_opts} do
      test_pid = self()
      max_retries = base_opts[:retry][:max_retries]
      {:ok, counter} = Agent.start_link(fn -> 0 end)
      bypass = Bypass.open(port: 9999)

      Bypass.expect(bypass, fn conn ->
        if Agent.get_and_update(counter, &{&1, &1 + 1}) < max_retries do
          send(test_pid, :pong)
          Plug.Conn.send_resp(conn, 503, "oops")
        else
          Plug.Conn.send_resp(conn, 200, "{}")
        end
      end)

      opts = Keyword.put(base_opts, :__internal_tesla_adapter__, Tesla.Adapter.Httpc)
      opts = Keyword.put(opts, :middleware, [{Tesla.Middleware.BaseUrl, "http://localhost:9999"}])
      {:ok, opts} = GoogleApiClient.init(opts)
      assert GoogleApiClient.acknowledge(["1", "2"], opts) == :ok

      assert_received :pong
      assert_received :pong
      assert_received :pong
      refute_received _
    end

    test "if the request fails, returns :ok and logs an error", %{pid: pid, opts: base_opts} do
      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 503, body: %{}}
      end)

      {:ok, opts} = GoogleApiClient.init(base_opts)

      assert capture_log(fn ->
               assert GoogleApiClient.acknowledge(["1", "2"], opts) == :ok
             end) =~ "[error] Unable to acknowledge messages with Cloud Pub/Sub, reason: "
    end
  end

  describe "put_deadline/3" do
    setup do
      test_pid = self()

      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(test_pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 200, body: @empty_response}
      end)

      %{
        pid: test_pid,
        opts: [
          __internal_tesla_adapter__: Tesla.Mock,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []},
          retry: [max_retries: 3, delay: 1]
        ]
      }
    end

    test "makes a projects.subscriptions.modifyAckDeadline request", %{
      opts: base_opts
    } do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      ack_ids = ["1", "2"]
      GoogleApiClient.put_deadline(ack_ids, 30, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"ackIds" => ack_ids, "ackDeadlineSeconds" => 30}

      assert url ==
               "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:modifyAckDeadline"
    end

    test "if the request fails, returns :ok and logs an error", %{pid: pid, opts: base_opts} do
      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 503, body: %{}}
      end)

      {:ok, opts} = GoogleApiClient.init(base_opts)

      assert capture_log(fn ->
               assert GoogleApiClient.put_deadline(["1", "2"], 60, opts) == :ok
             end) =~ "[error] Unable to put new ack deadline with Cloud Pub/Sub, reason: "
    end
  end

  describe "prepare_to_connect/2" do
    test "returns a child_spec for :hackney_pool" do
      {[pool_spec], opts} = GoogleApiClient.prepare_to_connect(SomePipeline, pool_size: 2)

      assert name = opts[:__connection_pool__]
      assert pool_spec == :hackney_pool.child_spec(name, max_connections: 2)
    end

    test "with extra options" do
      pool_opts = [timeout: 20_000]
      expected_pool_opts = Keyword.put(pool_opts, :max_connections, 5)
      client_opts = [pool_size: 5, pool_opts: pool_opts]

      {[pool_spec], opts} = GoogleApiClient.prepare_to_connect(SomePipeline, client_opts)

      assert name = opts[:__connection_pool__]
      assert pool_spec == :hackney_pool.child_spec(name, expected_pool_opts)
    end

    test "max_connections takes precedence over pool_size" do
      pool_opts = [timeout: 20_000, max_connections: 100]
      client_opts = [pool_size: 5, pool_opts: pool_opts]

      {[pool_spec], opts} = GoogleApiClient.prepare_to_connect(SomePipeline, client_opts)

      assert name = opts[:__connection_pool__]
      assert pool_spec == :hackney_pool.child_spec(name, pool_opts)
    end
  end

  describe "integration with BroadwayCloudPubSub.Acknowledger" do
    setup do
      test_pid = self()

      Tesla.Mock.mock(fn
        %{url: <<@subscription_base, action::binary>> = url} = req when action == ":pull" ->
          body_object = Poison.decode!(req.body)

          send(test_pid, {:pull_dispatched, %{url: url, body: body_object}})

          %Tesla.Env{status: 200, body: @pull_response}

        %{url: <<@subscription_base, action::binary>>} = req when action == ":acknowledge" ->
          %{"ackIds" => ack_ids} = Poison.decode!(req.body)

          send(test_pid, {:acknowledge_dispatched, length(ack_ids), ack_ids})

          %Tesla.Env{status: 200, body: @empty_response}

        %{url: <<@subscription_base, action::binary>>} = req
        when action == ":modifyAckDeadline" ->
          %{"ackIds" => ack_ids, "ackDeadlineSeconds" => deadline} = Poison.decode!(req.body)

          send(
            test_pid,
            {:modack_dispatched, length(ack_ids), deadline}
          )

          %Tesla.Env{status: 200, body: @empty_response}
      end)

      {:ok,
       %{
         pid: test_pid,
         opts: [
           client: GoogleApiClient,
           __internal_tesla_adapter__: Tesla.Mock,
           subscription: "projects/foo/subscriptions/bar",
           token_generator: {__MODULE__, :generate_token, []},
           retry: [max_retries: 3, delay: 1]
         ]
       }}
    end

    test "returns a list of Broadway.Message structs with ack builder", %{
      opts: base_opts
    } do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      [message1, message2, message3] = GoogleApiClient.receive_messages(10, &{:ack, &1}, opts)

      assert {:ack, _} = message1.acknowledger
      assert {:ack, _} = message2.acknowledger
      assert {:ack, _} = message3.acknowledger
    end

    test "with defaults successful messages are acknowledged, and failed messages are ignored", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} = init_with_ack_builder(base_opts)

      messages = GoogleApiClient.receive_messages(10, builder, opts)

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

      [_, _, _] = messages = GoogleApiClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      refute_receive {:acknowledge_dispatched, 3, _}
    end

    test "when :on_success is :nack, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, :nack)
        |> init_with_ack_builder()

      [_, _, _] = messages = GoogleApiClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      assert_receive {:modack_dispatched, 3, 0}
      refute_receive {:acknowledge_dispatched, 3, _}
    end

    test "when :on_success is {:nack, integer}, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_success, {:nack, 300})
        |> init_with_ack_builder()

      [_, _, _] = messages = GoogleApiClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, messages, [])

      assert_receive {:modack_dispatched, 3, 300}
      refute_receive {:acknowledge_dispatched, 3, _}
    end

    test "with default :on_failure, failed messages are ignored", %{opts: base_opts} do
      {ack_ref, builder, opts} = init_with_ack_builder(base_opts)

      [_, _, _] = messages = GoogleApiClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      refute_receive {:acknowledge_dispatched, 3, _}
    end

    test "when :on_failure is :nack, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_failure, :nack)
        |> init_with_ack_builder()

      [_, _, _] = messages = GoogleApiClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      assert_receive {:modack_dispatched, 3, 0}
    end

    test "when :on_failure is {:nack, integer}, dispatches modifyAckDeadline", %{
      opts: base_opts
    } do
      {ack_ref, builder, opts} =
        base_opts
        |> Keyword.put(:on_failure, {:nack, 60})
        |> init_with_ack_builder()

      [_, _, _] = messages = GoogleApiClient.receive_messages(10, builder, opts)

      Acknowledger.ack(ack_ref, [], messages)

      assert_receive {:modack_dispatched, 3, 60}
    end
  end

  def generate_token, do: {:ok, "token.#{System.os_time(:second)}"}
end
