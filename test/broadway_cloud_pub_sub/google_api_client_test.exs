defmodule BroadwayCloudPubSub.GoogleApiClientTest do
  use ExUnit.Case

  import ExUnit.CaptureLog

  alias BroadwayCloudPubSub.GoogleApiClient
  alias Broadway.Message

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

  @acknowledge_response """
  {}
  """

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

    test ":scope should be a string" do
      opts = [subscription: "projects/foo/subscriptions/bar"]

      {:ok, result} = opts |> Keyword.put(:scope, "https://example.com") |> GoogleApiClient.init()

      assert {_, _, ["https://example.com"]} = result.token_generator

      {:error, message} = opts |> Keyword.put(:scope, :an_atom) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string, got: :an_atom"

      {:error, message} = opts |> Keyword.put(:scope, 1) |> GoogleApiClient.init()

      assert message == "expected :scope to be a non empty string, got: 1"
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

      assert {:ok, result} =
               opts
               |> Keyword.put(:scope, :an_invalid_scope)
               |> Keyword.put(:token_generator, {__MODULE__, :generate_token, []})
               |> GoogleApiClient.init()
    end
  end

  describe "receive_messages/2" do
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
          token_generator: {__MODULE__, :generate_token, []}
        ]
      }
    end

    test "returns a list of Broadway.Message with :data, :metadata, and :acknowledger set", %{
      opts: base_opts
    } do
      {:ok, opts} = GoogleApiClient.init(base_opts)
      [message1, message2, message3] = GoogleApiClient.receive_messages(10, opts)
      ack_data = %{ack_id: "1", on_failure: :noop, on_success: :ack}

      assert %Message{data: "Message1", metadata: %{publishTime: %DateTime{}}} = message1

      assert message1.metadata.messageId == "19917247034"

      assert %{
               "foo" => "bar",
               "qux" => ""
             } = message1.metadata.attributes

      assert message1.acknowledger == {GoogleApiClient, opts.ack_ref, ack_data}

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
               assert GoogleApiClient.receive_messages(10, opts) == []
             end) =~ "[error] Unable to fetch events from Cloud Pub/Sub. Reason: "
    end

    test "send a projects.subscriptions.pull request with default options", %{opts: base_opts} do
      {:ok, opts} = GoogleApiClient.init(base_opts)
      GoogleApiClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"maxMessages" => 10}
      assert url == "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:pull"
    end

    test "request with custom :return_immediately", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:return_immediately, true) |> GoogleApiClient.init()
      GoogleApiClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body["returnImmediately"] == true
    end

    test "request with custom :max_number_of_messages", %{opts: base_opts} do
      {:ok, opts} = base_opts |> Keyword.put(:max_number_of_messages, 5) |> GoogleApiClient.init()
      GoogleApiClient.receive_messages(10, opts)

      assert_received {:http_request_called, %{body: body, url: _url}}
      assert body["maxMessages"] == 5
    end
  end

  describe "configure/3" do
    test "raise on unsupported configure option" do
      assert_raise(ArgumentError, "unsupported configure option :on_other", fn ->
        GoogleApiClient.configure(:channel, %{}, on_other: :ack)
      end)
    end

    test "raise on unsupported on_success value" do
      error_msg = "expected :on_success to be a valid on_success value, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        GoogleApiClient.configure(:channel, %{}, on_success: :unknown)
      end)
    end

    test "raise on unsupported on_failure value" do
      error_msg = "expected :on_failure to be a valid on_failure value, got: :unknown"

      assert_raise(ArgumentError, error_msg, fn ->
        GoogleApiClient.configure(:channel, %{}, on_failure: :unknown)
      end)
    end

    test "set on_success correctly" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_success: :ack}

      assert {:ok, expected} == GoogleApiClient.configure(:channel, ack_data, on_success: :ack)
    end

    test "set on_success with noop" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_success: :noop}

      assert {:ok, expected} == GoogleApiClient.configure(:channel, ack_data, on_success: :noop)
    end

    test "set on_failure with deadline 0" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_failure: {:nack, 0}}

      assert {:ok, expected} == GoogleApiClient.configure(:channel, ack_data, on_failure: :nack)
    end

    test "set on_failure with custom deadline" do
      ack_data = %{ack_id: "1"}
      expected = %{ack_id: "1", on_failure: {:nack, 60}}

      assert {:ok, expected} ==
               GoogleApiClient.configure(:channel, ack_data, on_failure: {:nack, 60})
    end
  end

  describe "ack/2" do
    setup do
      test_pid = self()

      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(test_pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 200, body: @acknowledge_response}
      end)

      %{
        pid: test_pid,
        opts: [
          __internal_tesla_adapter__: Tesla.Mock,
          subscription: "projects/foo/subscriptions/bar",
          token_generator: {__MODULE__, :generate_token, []}
        ]
      }
    end

    test "send a projects.subscriptions.acknowledge request", %{opts: base_opts} do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      messages = test_messages(opts)

      GoogleApiClient.ack(opts.ack_ref, messages, [])

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body == %{"ackIds" => ["1", "2"]}
      assert url == "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:acknowledge"
    end

    test "with no successful messages, by default is a no-op", %{opts: base_opts} do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      messages = test_messages(opts, data: {:failed, :test})
      GoogleApiClient.ack(opts.ack_ref, [], messages)

      refute_received {:http_request_called, _}
    end

    test "with no successful messages, send projects.subscriptions.modifyAckDeadline", %{
      opts: base_opts
    } do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      messages = test_messages(opts, on_failure: {:nack, 0}, data: {:failed, :test})
      GoogleApiClient.ack(opts.ack_ref, [], messages)

      assert_received {:http_request_called, %{body: body, url: url}}
      assert body == %{"ackIds" => ["1", "2"], "ackDeadlineSeconds" => 0}

      assert url ==
               "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:modifyAckDeadline"
    end

    test "if the request fails, returns :ok and logs the error", %{pid: pid, opts: base_opts} do
      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 503, body: %{}}
      end)

      {:ok, opts} = GoogleApiClient.init(base_opts)

      messages = test_messages(opts)

      assert capture_log(fn ->
               assert GoogleApiClient.ack(opts.ack_ref, messages, []) == :ok
             end) =~ "[error] Unable to acknowledge messages with Cloud Pub/Sub. Reason: "
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

  def generate_token, do: {:ok, "token.#{System.os_time(:second)}"}

  defp test_messages(client_opts, opts \\ []) do
    data = opts[:data]
    on_success = opts[:on_success] || :ack
    on_failure = opts[:on_failure] || :noop

    [
      %Message{
        acknowledger:
          {GoogleApiClient, client_opts.ack_ref,
           %{ack_id: "1", on_success: on_success, on_failure: on_failure}},
        data: data
      },
      %Message{
        acknowledger:
          {GoogleApiClient, client_opts.ack_ref,
           %{ack_id: "2", on_success: on_success, on_failure: on_failure}},
        data: data
      }
    ]
  end
end
