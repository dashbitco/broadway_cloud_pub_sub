defmodule BroadwayCloudPubSub.ClientAcknowledger do
  # This module implements the `Broadway.Acknowledger` behaviour,
  # using the client for communication with Google Cloud Pub/Sub.
  #
  # ## Handling acknowledgements
  #
  # If you are writing a Pub/Sub client, instead of implementing the
  # `Broadway.Acknowledger` behaviour directly, you can choose to integrate with
  # the ClientAcknowledger. This will allow users of your client to use the same
  # options and get the same behavior as outlined in the "Acknowledgements"
  # section of the `BroadwayCloudPubSub.Producer` docs.

  # To use the ClientAcknowledger with your client, initialize its configuration
  # and build your client's `ack_ref` in `c:init/1`:

  #     defmodule MyPubsubClient do
  #       alias BroadwayCloudPubSub.Client
  #       alias BroadwayCloudPubSub.ClientAcknowledger

  #       @behaviour Client

  #       @impl true
  #       def init(opts) do
  #         with {:ok, config} <- validate_config(opts),
  #              {:ok, ack} <- ClientAcknowledger.init(opts) do
  #           # Build an ack_ref with your client config
  #           ack_ref = ClientAcknowledger.ack_ref(ack, config)

  #           {:ok, %{config | ack_ref: ack_ref}}
  #         end
  #       end

  # In `c:receive_messages/2`, build the `acknowledger` for each Message using the
  # `ackId` from Cloud Pub/Sub message:

  #       @impl true
  #       def receive_messages(demand, config) do
  #         case do_receive_messages(demand, config) do
  #           {:ok, received_messages} ->
  #             Enum.map(received_messages, fn message ->
  #               %Broadway.Message{
  #                 data: message.data,
  #                 acknowledger: ClientAcknowledger.acknowledger(message.ackId, config.ack_ref)
  #               }
  #             end)
  #           _ ->
  #             []
  #         end
  #       end

  # Finally, implement the optional callbacks `c:acknowledge/2`:

  #       @impl true
  #       def acknowledge(ack_ids, config) do
  #         # dispatch an acknowledge request
  #       end

  # and `c:put_deadline/3`:

  #       @impl true
  #       def put_deadline(ack_ids, new_deadline, config) do
  #         # dispatch a modifyAckDeadline request
  #       end
  #     end

  # These callbacks will be used by the ClientAcknowledger to dispatch requests
  # to Google Cloud Pub/Sub.
  @moduledoc false
  alias Broadway.{Acknowledger, TermStorage}
  alias BroadwayCloudPubSub.Client

  @behaviour Acknowledger

  @typedoc """
  Acknowledgement data for a `Broadway.Message`.
  """
  @type ack_data :: %{
          :ack_id => String.t(),
          optional(:on_failure) => ack_option,
          optional(:on_success) => ack_option
        }

  @typedoc """
  An acknowledgement action.
  """
  @type ack_option :: :ack | :noop | :nack | {:nack, Client.ack_deadline()}

  @type ack_ref :: reference

  @type t :: %__MODULE__{
          :client => module,
          :client_opts => any,
          :on_failure => ack_option,
          :on_success => ack_option
        }

  @enforce_keys [:client]
  defstruct [:client, :client_opts, on_failure: :noop, on_success: :ack]

  # The maximum number of ackIds to be sent in acknowledge/modifyAckDeadline
  # requests. There is an API limit of 524288 bytes (512KiB) per acknowledge/modifyAckDeadline
  # request. ackIds have a maximum size of 164 bytes, so 524288/164 ~= 3197.
  # Accounting for some overhead, a maximum of 3000 ackIds per request should be safe.
  # See https://github.com/googleapis/nodejs-pubsub/pull/65/files#diff-3d29c4447546c72118ed5d5cbf38ab8bR34-R42
  @max_ack_ids_per_request 3_000

  @doc """
  Initializes this acknowledger for use with a `BroadwayCloudPubSub.Client`.

  ## Options

  The following options are usually provided by `BroadwayCloudPubSub.Producer`:

    * `client` - The client module integrating with the #{inspect(__MODULE__)}.

    * `on_success` - Optional. The action to perform for successful messages. Default is `:ack`.

    * `on_failure` - The action to perform for failed messages. Default is `:noop`.
  """
  @spec init(opts :: any) :: {:ok, t} | {:error, message :: binary}
  def init(opts) do
    with {:ok, client} <- validate(opts, :client),
         {:ok, on_success} <- validate(opts, :on_success, :ack),
         {:ok, on_failure} <- validate(opts, :on_failure, :noop) do
      {:ok,
       %__MODULE__{
         client: client,
         on_failure: on_failure,
         on_success: on_success
       }}
    end
  end

  @doc """
  Creates a reference that can be passed to `acknowledger/2`.

  The client configuration is stored in `Broadway.TermStorage`.
  """
  @spec ack_ref(config :: t, client_opts :: any) :: ack_ref
  def ack_ref(config, client_opts) do
    TermStorage.put(%{config | client_opts: client_opts})
  end

  @doc """
  Returns an `acknowledger` to be put on a `Broadway.Message`.

  ## Example

      iex> BroadwayCloudPubSub.ClientAcknowledger.acknowledger("ackId", :ack_ref)
      {BroadwayCloudPubSub.ClientAcknowledger, :ack_ref, %{ack_id: "ackId"}}

  """
  @spec acknowledger(ack_id :: Client.ack_id(), ack_ref) :: {__MODULE__, ack_ref, ack_data}
  def acknowledger(ack_id, ack_ref) do
    {__MODULE__, ack_ref, %{ack_id: ack_id}}
  end

  @impl Acknowledger
  def ack(ack_ref, successful, failed) do
    config = TermStorage.get!(ack_ref)

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
    %__MODULE__{client: client, client_opts: opts} = config

    client.acknowledge(ack_ids, opts)
  end

  defp apply_ack_func({:nack, deadline}, ack_ids, config) do
    %__MODULE__{client: client, client_opts: opts} = config

    client.put_deadline(ack_ids, deadline, opts)
  end

  defp validate(opts, key, default \\ nil) when is_list(opts) do
    validate_option(key, opts[key] || default)
  end

  defp validate_option(:client, client) when not is_atom(client) do
    validation_error(:client, "a module implementing #{inspect(Client)}", client)
  end

  defp validate_option(:client, client) when client in [true, nil] do
    validation_error(:client, "a module implementing #{inspect(Client)}", client)
  end

  defp validate_option(:client, client) do
    with {:loaded, true} <- {:loaded, Code.ensure_compiled?(client)},
         {:acknowledger, {:error, _}} <- {:acknowledger, validate_exported(client, :ack, 3)},
         {:ok, _} <- validate_exported(client, :acknowledge, 2),
         {:ok, _} <- validate_exported(client, :put_deadline, 3) do
      {:ok, client}
    else
      {:loaded, false} ->
        {:error, "the client #{inspect(client)} does not exist or could not be loaded"}

      {:acknowledger, _} ->
        {:error,
         "the client #{inspect(client)} is attempting to call #{inspect(__MODULE__)}.init/1, but the client itself implements the #{
           inspect(Acknowledger)
         } behaviour"}

      other ->
        other
    end
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

  defp validate_exported(client, function, arity) do
    if function_exported?(client, function, arity) do
      {:ok, client}
    else
      {:error, "#{inspect(client)}.#{function}/#{arity} is undefined or private"}
    end
  end

  defp validation_error(option, expected, value) do
    {:error, "expected #{inspect(option)} to be #{expected}, got: #{inspect(value)}"}
  end

  defp validate_action(:ack), do: {:ok, :ack}
  defp validate_action(:noop), do: {:ok, :noop}
  defp validate_action(:nack), do: {:ok, {:nack, 0}}
  defp validate_action({:nack, n}) when is_integer(n) and n >= 0, do: {:ok, {:nack, n}}

  # TODO: remove in v0.7.0
  defp validate_action(:ignore) do
    IO.warn(":on_success/:on_failure value :ignore is deprecated in favour of :noop")
    {:ok, :noop}
  end

  defp validate_action(_), do: :error
end
