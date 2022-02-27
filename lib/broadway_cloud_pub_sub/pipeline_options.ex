defmodule BroadwayCloudPubSub.PipelineOptions do
  @moduledoc false
  require Logger

  @default_max_number_of_messages 10
  @default_receive_timeout :infinity

  @default_scope "https://www.googleapis.com/auth/pubsub"

  def validate(opts) do
    with(
      {:ok, subscription} <- validate_subscription(opts),
      {:ok, token_generator} <- validate_token_opts(opts),
      {:ok, pull_request} <- validate_pull_request(opts),
      {:ok, receive_timeout} <- validate(opts, :receive_timeout, @default_receive_timeout)
    ) do
      config = %{
        subscription: subscription,
        token_generator: token_generator,
        pull_request: pull_request,
        receive_timeout: receive_timeout
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

  defp validate_option(:receive_timeout, :infinity), do: {:ok, :infinity}

  defp validate_option(:receive_timeout, value) when not is_integer(value) or value < 0,
    do: validation_error(:receive_timeout, "a non-negative integer or :infinity", value)

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
