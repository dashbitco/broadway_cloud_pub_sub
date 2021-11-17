defmodule BroadwayCloudPubSub.PipelineOptions do
  @moduledoc false
  alias NimbleOptions.ValidationError
  require Logger

  @default_base_url "https://pubsub.googleapis.com"

  @default_max_number_of_messages 10
  @default_receive_timeout :infinity

  @default_receive_interval 5_000

  @default_scope "https://www.googleapis.com/auth/pubsub"

  definition = [
    # Internal.
    finch_name: [type: :atom, doc: false],
    # Handled by Broadway.
    broadway: [type: :any, doc: false],
    client: [
      type: :atom,
      default: BroadwayCloudPubSub.PullClient,
      doc: """
      A module that implements the BroadwayCloudPubSub.Client behaviour.
      This module is responsible for fetching and acknowledging the messages.
      Pay attention that all options passed to the producer will be forwarded
      to the client. It's up to the client to normalize the options it needs.
      """
    ],
    subscription: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :subscription}]]},
      required: true,
      doc: """
      The name of the subscription, including the project.
      For example, if you project is named `"my-project"` and your
      subscription is named `"my-subscription"`, then your subscription
      name is `"projects/my-project/subscriptions/my-subscription"`.
      """
    ],
    max_number_of_messages: [
      doc: "The maximum number of messages to be fetched per request.",
      type: :pos_integer,
      default: @default_max_number_of_messages
    ],
    on_failure: [
      type:
        {:custom, __MODULE__, :type_atom_action_or_nack_with_bounded_integer,
         [[{:name, :on_failure}, {:min, 0}, {:max, 600}]]},
      doc: """
      Configures the acking behaviour for failed messages.
      See the "Acknowledgements" section below for all the possible values.
      This option can also be changed for each message through
      `Broadway.Message.configure_ack/2`.
      """,
      default: :noop
    ],
    on_success: [
      type:
        {:custom, __MODULE__, :type_atom_action_or_nack_with_bounded_integer,
         [[{:name, :on_success}, {:min, 0}, {:max, 600}]]},
      doc: """
      Configures the acking behaviour for successful messages.
      See the "Acknowledgements" section below for all the possible values.
      This option can also be changed for each message through
      `Broadway.Message.configure_ack/2`.
      """,
      default: :ack
    ],
    receive_interval: [
      type: :integer,
      default: @default_receive_interval,
      doc: """
      The duration (in milliseconds) for which the producer waits
      before making a request for more messages.
      """
    ],
    scope: [
      type: {:custom, __MODULE__, :type_non_empty_string_or_tagged_tuple, [[{:name, :scope}]]},
      default: @default_scope,
      doc: """
      A string representing the scope or scopes to use when fetching
      an access token. Note that this option only applies to the
      default token generator.
      """
    ],
    token_generator: [
      type: :mfa,
      doc: """
      An MFArgs tuple that will be called before each request
      to fetch an authentication token. It should return `{:ok, String.t()} | {:error, any()}`.
      By default this will invoke `Goth.Token.for_scope/1` with `"#{@default_scope}"`.
      See the "Custom token generator" section below for more information.
      """
    ],
    base_url: [
      type: {:custom, __MODULE__, :type_non_empty_string, [[{:name, :base_url}]]},
      default: @default_base_url,
      doc: """
      The base URL for the Cloud Pub/Sub service.
      This option is mostly useful for testing via the Pub/Sub emulator.
      """
    ],
    pool_size: [
      type: :pos_integer,
      doc: """
      The size of the connection pool.
      The default value is twice the producer concurrency.
      """
    ],
    return_immediately: [
      doc: false,
      deprecated: "Google Pub/Sub will remove this option in the future.",
      type: :boolean
    ],
    # Testing Options
    test_pid: [
      type: :pid,
      doc: false
    ],
    message_server: [
      type: :pid,
      doc: false
    ]
  ]

  @definition NimbleOptions.new!(definition)

  def definition do
    @definition
  end

  @acknowledger_definition definition
                           |> Keyword.take([:on_failure, :on_success])
                           |> NimbleOptions.new!()

  def acknowledger_definition do
    @acknowledger_definition
  end

  @doc """
  Builds an MFArgs tuple for a token generator.
  """
  def make_token_generator(opts) do
    scope = Keyword.fetch!(opts, :scope)
    ensure_goth_loaded()
    {__MODULE__, :generate_goth_token, [scope]}
  end

  defp ensure_goth_loaded() do
    unless Code.ensure_loaded?(Goth.Token) do
      Logger.error("""
      the default authentication token generator uses the Goth library but it's not available

      Add goth to your dependencies:

          defp deps() do
            {:goth, "~> 1.0"}
          end

      Or provide your own token generator:

          Broadway.start_link(
            producers: [
              default: [
                module: {BroadwayCloudPubSub.Producer,
                  token_generator: {MyGenerator, :generate, ["foo"]}
                }
              ]
            ]
          )
      """)
    end
  end

  def type_atom_action_or_nack_with_bounded_integer(:ack, [{:name, _}, {:min, _}, {:max, _}]) do
    {:ok, :ack}
  end

  def type_atom_action_or_nack_with_bounded_integer(:noop, [{:name, _}, {:min, _}, {:max, _}]) do
    {:ok, :noop}
  end

  def type_atom_action_or_nack_with_bounded_integer(:nack, [{:name, _}, {:min, _}, {:max, _}]) do
    {:ok, {:nack, 0}}
  end

  def type_atom_action_or_nack_with_bounded_integer(
        {:nack, value},
        [{:name, _}, {:min, min}, {:max, max}]
      )
      when is_integer(value) and value >= min and value <= max do
    {:ok, {:nack, value}}
  end

  def type_atom_action_or_nack_with_bounded_integer(value, [
        {:name, name},
        {:min, min},
        {:max, max}
      ]) do
    {:error,
     "expected :#{name} to be one of :ack, :noop, :nack, or {:nack, integer} where " <>
       "integer is between #{min} and #{max}, got: #{inspect(value)}"}
  end

  def type_non_empty_string("", [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: \"\""}
  end

  def type_non_empty_string(value, _)
      when not is_nil(value) and is_binary(value) do
    {:ok, value}
  end

  def type_non_empty_string(value, [{:name, name}]) do
    {:error, "expected :#{name} to be a non-empty string, got: #{inspect(value)}"}
  end

  def type_non_empty_string_or_tagged_tuple(value, [{:name, name}])
      when not (is_binary(value) or is_tuple(value)) or (value == "" or value == {}) do
    {:error, "expected :#{name} to be a non-empty string or tuple, got: #{inspect(value)}"}
  end

  def type_non_empty_string_or_tagged_tuple(value, _) do
    {:ok, value}
  end

  def format_error(%ValidationError{keys_path: [], message: message}, mod, fun, arity) do
    "invalid configuration given to #{inspect(mod)}.#{fun}/#{arity}, " <> message
  end

  def format_error(%ValidationError{keys_path: keys_path, message: message}, mod, fun, arity) do
    "invalid configuration given to #{inspect(mod)}.#{fun}/#{arity} for key #{inspect(keys_path)}, " <>
      message
  end

  @doc false
  def generate_goth_token(scope) do
    with {:ok, %{token: token}} <- Goth.Token.for_scope(scope) do
      {:ok, token}
    end
  end
end
