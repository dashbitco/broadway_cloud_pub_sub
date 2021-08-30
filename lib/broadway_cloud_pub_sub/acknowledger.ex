defmodule BroadwayCloudPubSub.Acknowledger do
  @moduledoc false
  alias Broadway.Acknowledger
  alias BroadwayCloudPubSub.Client

  @behaviour Acknowledger

  @typedoc """
  Acknowledgement data for a `Broadway.Message`.
  """
  @type ack_data :: %{
          :ack_id => Client.ack_id(),
          optional(:on_failure) => ack_option,
          optional(:on_success) => ack_option
        }

  @typedoc """
  An acknowledgement action.
  """
  @type ack_option :: :ack | :noop | :nack | {:nack, Client.ack_deadline()}

  @type ack_ref :: reference

  @type t :: %__MODULE__{
          client: module,
          client_config: any,
          on_failure: ack_option,
          on_success: ack_option
        }

  @enforce_keys [:client]
  defstruct [:client, :client_config, on_failure: :noop, on_success: :ack]

  # The maximum number of ackIds to be sent in acknowledge/modifyAckDeadline
  # requests. There is an API limit of 524288 bytes (512KiB) per acknowledge/modifyAckDeadline
  # request. ackIds have a maximum size of 184 bytes, so 524288/184 ~= 2849.
  # Accounting for some overhead, a maximum of 2500 ackIds per request should be safe.
  # See https://github.com/googleapis/nodejs-pubsub/pull/65/files#diff-3d29c4447546c72118ed5d5cbf38ab8bR34-R42
  @max_ack_ids_per_request 2_500

  @doc """
  Initializes this acknowledger for use with a `BroadwayCloudPubSub.Client`.

  ## Options

  The following options are usually provided by `BroadwayCloudPubSub.Producer`:

    * `client` - The client module integrating with the #{inspect(__MODULE__)}.

    * `on_success` - Optional. The action to perform for successful messages. Default is `:ack`.

    * `on_failure` - The action to perform for failed messages. Default is `:noop`.
  """
  @spec init(module, term, keyword) :: {:ok, ack_ref} | {:error, message :: binary}
  def init(client, config, opts) do
    with {:ok, on_success} <- validate(opts, :on_success, :ack),
         {:ok, on_failure} <- validate(opts, :on_failure, :noop) do
      state = %__MODULE__{
        client: client,
        client_config: config,
        on_failure: on_failure,
        on_success: on_success
      }

      ack_ref = make_ref()
      put_config(ack_ref, state)

      {:ok, ack_ref}
    end
  end

  defp put_config(reference, state) do
    :persistent_term.put({__MODULE__, reference}, state)
  end

  @spec get_config(ack_ref) :: t()
  def get_config(reference) do
    :persistent_term.get({__MODULE__, reference})
  end

  @doc """
  Returns an `acknowledger` to be put on a `Broadway.Message`.
  """
  @spec builder(ack_ref) :: (Client.ack_id() -> {__MODULE__, ack_ref, ack_data})
  def builder(ack_ref) do
    &{__MODULE__, ack_ref, %{ack_id: &1}}
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    config = get_config(ack_ref)

    success_actions = group_actions_ack_ids(successful, :on_success, config)
    failure_actions = group_actions_ack_ids(failed, :on_failure, config)

    success_actions
    |> Map.merge(failure_actions, fn _, a, b -> a ++ b end)
    |> ack_messages(config)

    :ok
  end

  @impl Acknowledger
  def configure(_ack_ref, ack_data, options) do
    options = assert_valid_config!(options)
    ack_data = Map.merge(ack_data, Map.new(options))
    {:ok, ack_data}
  end

  defp assert_valid_config!(options) do
    Enum.map(options, fn
      {:on_success, value} -> {:on_success, validate_option!(:on_success, value)}
      {:on_failure, value} -> {:on_failure, validate_option!(:on_failure, value)}
      {other, _value} -> raise ArgumentError, "unsupported configure option #{inspect(other)}"
    end)
  end

  defp group_actions_ack_ids(messages, key, config) do
    Enum.group_by(messages, &group_acknowledger(&1, key, config), &extract_ack_id/1)
  end

  defp group_acknowledger(%{acknowledger: {_, _, ack_data}}, key, config) do
    Map.get_lazy(ack_data, key, fn -> config_action(key, config) end)
  end

  defp config_action(:on_success, %{on_success: action}), do: action
  defp config_action(:on_failure, %{on_failure: action}), do: action

  defp extract_ack_id(message) do
    {_, _, %{ack_id: ack_id}} = message.acknowledger
    ack_id
  end

  defp ack_messages(actions_and_ids, config) do
    Enum.each(actions_and_ids, fn {action, ack_ids} ->
      ack_ids
      |> Enum.chunk_every(@max_ack_ids_per_request)
      |> Enum.each(&apply_ack_func(action, &1, config))
    end)
  end

  defp apply_ack_func(:noop, _ack_ids, _config), do: :ok

  defp apply_ack_func(:ack, ack_ids, config) do
    %__MODULE__{client: client, client_config: opts} = config
    client.acknowledge(ack_ids, opts)
  end

  defp apply_ack_func({:nack, deadline}, ack_ids, config) do
    %__MODULE__{client: client, client_config: opts} = config
    client.put_deadline(ack_ids, deadline, opts)
  end

  defp validate(opts, key, default) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(action, value) when action in [:on_success, :on_failure] do
    case validate_action(value) do
      {:ok, result} -> {:ok, result}
      :error -> validation_error(action, "a valid acknowledgement option", value)
    end
  end

  defp validate_option(_, value), do: {:ok, value}

  defp validate_option!(key, value) do
    case validate_option(key, value) do
      {:ok, value} -> value
      {:error, message} -> raise ArgumentError, message
    end
  end

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_action(:ack), do: {:ok, :ack}
  defp validate_action(:noop), do: {:ok, :noop}
  defp validate_action(:nack), do: {:ok, {:nack, 0}}
  defp validate_action({:nack, n}) when is_integer(n) and n >= 0, do: {:ok, {:nack, n}}
  defp validate_action(_), do: :error
end
