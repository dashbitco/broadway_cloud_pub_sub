defmodule BroadwayCloudPubSub.PipelineOptions do
  @moduledoc false
  require Logger

  @default_max_number_of_messages 10

  @default_scope "https://www.googleapis.com/auth/pubsub"

  @valid_ack_values [:ack, :nack, :noop]

  # TODO: validate {:nack, integer()} as an ack value
  def definition do
    [
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
        type: :string,
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
        type: :integer,
        default: 10
      ],
      on_failure: [
        type: {:in, @valid_ack_values},
        doc: """
        Configures the acking behaviour for failed messages.
        See the "Acknowledgements" section below for all the possible values.
        This option can also be changed for each message through
        `Broadway.Message.configure_ack/2`.
        """,
        default: :noop
      ],
      on_success: [
        type: {:in, @valid_ack_values},
        doc: """
        Configures the acking behaviour for successful messages.
        See the "Acknowledgements" section below for all the possible values.
        This option can also be changed for each message through
        `Broadway.Message.configure_ack/2`.
        """,
        default: :ack
      ],
      pool_size: [
        type: {:custom, __MODULE__, :validate_pool_size, []},
        doc: """
        The size of the connection pool.
        The default value is twice the producer concurrency.
        """
      ],
      receive_interval: [
        type: :integer,
        default: 5_000,
        doc: """
        The duration (in milliseconds) for which the producer waits
        before making a request for more messages.
        """
      ],
      scope: [
        type: :string,
        default: "https://www.googleapis.com/auth/pubsub",
        doc: """
        A string representing the scope or scopes to use when fetching
        an access token. Note that the `:scope` option only applies to the
        default token generator.
        """
      ],
      token_generator: [
        type: :mfa,
        default: {__MODULE__, :generate_goth_token, []},
        doc: """
        An MFArgs tuple that will be called before each request
        to fetch an authentication token. It should return `{:ok, String.t()} | {:error, any()}`.
        By default this will invoke `Goth.Token.for_scope/1` with `"https://www.googleapis.com/auth/pubsub"`.
        See the "Custom token generator" section below for more information.
        """
      ],
      base_url: [
        doc: "The base URL for the Cloud PubSub services.",
        type: :string,
        default: "https://pubsub.googleapis.com"
      ]
    ]
  end

  def validate_pool_size(opts) do
    IO.inspect(opts, label: :pool_size)
    2
  end

  def validate(opts) do
    with {:ok, opts} <- NimbleOptions.validate(opts, definition()) do
      config = %{
        subscription: Keyword.fetch!(opts, :subscription),
        token_generator: Keyword.fetch!(opts, :token_generator),
        pull_request: Keyword.fetch!(opts, :pull_request)
      }

      {:ok, config}
    end
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:token_generator, {m, f, args})
       when is_atom(m) and is_atom(f) and is_list(args) do
    {:ok, {m, f, args}}
  end

  defp validate_option(:token_generator, value),
    do: validation_error(:token_generator, "a tuple {Mod, Fun, Args}", value)

  defp validate_option(:scope, value)
       when not (is_binary(value) or is_tuple(value)) or (value == "" or value == {}),
       do: validation_error(:scope, "a non empty string or tuple", value)

  defp validate_option(:subscription, value) when not is_binary(value) or value == "",
    do: validation_error(:subscription, "a non empty string", value)

  defp validate_option(:max_number_of_messages, value) when not is_integer(value) or value < 1,
    do: validation_error(:max_number_of_messages, "a positive integer", value)

  defp validate_option(:return_immediately, nil), do: {:ok, nil}

  defp validate_option(:return_immediately, value) when not is_boolean(value),
    do: validation_error(:return_immediately, "a boolean value", value)

  defp validate_option(_, value), do: {:ok, value}

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_pull_request(opts) do
    with {:ok, return_immediately} <- validate(opts, :return_immediately),
         {:ok, max_number_of_messages} <-
           validate(opts, :max_number_of_messages, @default_max_number_of_messages) do
      {:ok,
       %{
         maxMessages: max_number_of_messages,
         returnImmediately: return_immediately
       }}
    end
  end

  defp validate_token_opts(opts) do
    case Keyword.fetch(opts, :token_generator) do
      {:ok, _} -> validate(opts, :token_generator)
      :error -> validate_scope(opts)
    end
  end

  defp validate_scope(opts) do
    with {:ok, scope} <- validate(opts, :scope, @default_scope) do
      ensure_goth_loaded()
      {:ok, {__MODULE__, :generate_goth_token, [scope]}}
    end
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

  defp validate_subscription(opts) do
    with {:ok, subscription} <- validate(opts, :subscription) do
      subscription |> String.split("/") |> validate_sub_parts(subscription)
    end
  end

  defp validate_sub_parts(
         ["projects", projects_id, "subscriptions", subscriptions_id] = parts,
         _subscription
       ) do
    string = Enum.join(parts, "/")

    {:ok, %{projects_id: projects_id, subscriptions_id: subscriptions_id, string: string}}
  end

  defp validate_sub_parts(_, subscription) do
    validation_error(:subscription, "an valid subscription name", subscription)
  end

  @doc false
  def generate_goth_token(scope) do
    with {:ok, %{token: token}} <- Goth.Token.for_scope(scope) do
      {:ok, token}
    end
  end
end
