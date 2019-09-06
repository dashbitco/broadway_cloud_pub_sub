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

      assert %Message{data: "Message1", metadata: %{publishTime: %DateTime{}}} = message1

      assert message1.metadata.messageId == "19917247034"

      assert %{
               "foo" => "bar",
               "qux" => ""
             } = message1.metadata.attributes

      assert message1.acknowledger == {GoogleApiClient, opts.ack_ref, "1"}

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

  describe "wrap_received_messages/2" do
    test "allows string keys for messages" do
      message = %{
        "ackId" => "THE ACK ID",
        "message" => %{
          "attributes" => %{"space" => "vegans"},
          "data" => "VEhJUyBJUyBBIFRFU1Q=",
          "messageId" => "111110000222233344",
          "publishTime" => "2019-09-05T17:41:31.520Z"
        }
      }

      messages = {:ok, %{receivedMessages: [message]}}

      result = [
        %Broadway.Message{
          acknowledger: {BroadwayCloudPubSub.GoogleApiClient, "Ack Ref", "THE ACK ID"},
          batch_key: :default,
          batch_mode: :bulk,
          batcher: :default,
          data: "THIS IS A TEST",
          metadata: %{},
          status: :ok
        }
      ]

      assert GoogleApiClient.wrap_received_messages(messages, "Ack Ref") == result
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

      GoogleApiClient.ack(
        opts.ack_ref,
        [
          %Message{acknowledger: {GoogleApiClient, opts.ack_ref, "1"}, data: nil},
          %Message{acknowledger: {GoogleApiClient, opts.ack_ref, "2"}, data: nil}
        ],
        []
      )

      assert_received {:http_request_called, %{body: body, url: url}}

      assert body == %{"ackIds" => ["1", "2"]}
      assert url == "https://pubsub.googleapis.com/v1/projects/foo/subscriptions/bar:acknowledge"
    end

    test "with no successful messages, is a no-op", %{opts: base_opts} do
      {:ok, opts} = GoogleApiClient.init(base_opts)

      GoogleApiClient.ack(
        opts.ack_ref,
        [],
        [
          %Message{
            acknowledger: {GoogleApiClient, opts.ack_ref, "1"},
            data: nil,
            status: {:failed, :test}
          },
          %Message{
            acknowledger: {GoogleApiClient, opts.ack_ref, "2"},
            data: nil,
            status: {:failed, :test}
          }
        ]
      )

      refute_received {:http_request_called, _}
    end

    test "if the request fails, returns :ok and logs the error", %{pid: pid, opts: base_opts} do
      Tesla.Mock.mock(fn %{method: :post} = req ->
        body_object = Poison.decode!(req.body)
        send(pid, {:http_request_called, %{url: req.url, body: body_object}})

        %Tesla.Env{status: 503, body: %{}}
      end)

      {:ok, opts} = GoogleApiClient.init(base_opts)

      assert capture_log(fn ->
               assert GoogleApiClient.ack(
                        opts.ack_ref,
                        [
                          %Message{acknowledger: {GoogleApiClient, opts.ack_ref, "1"}, data: nil},
                          %Message{acknowledger: {GoogleApiClient, opts.ack_ref, "2"}, data: nil}
                        ],
                        []
                      ) == :ok
             end) =~ "[error] Unable to acknowledge messages with Cloud Pub/Sub. Reason: "
    end
  end

  def generate_token, do: {:ok, "token.#{System.os_time(:second)}"}
end
