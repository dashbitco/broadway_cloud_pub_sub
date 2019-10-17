defmodule BroadwayCloudPubSub.Client do
  @moduledoc """
  A generic behaviour to implement Pub/Sub Clients for `BroadwayCloudPubSub.Producer`.

  This module defines callbacks to normalize options and receive messages
  from a Cloud Pub/Sub topic. Modules that implement this behaviour should be passed
  as the `:client` option from `BroadwayCloudPubSub.Producer`.
  """

  alias Broadway.Message

  @type messages :: [Message.t()]

  @doc """
  Invoked once by BroadwayCloudPubSub.Producer during `Broadway.start_link/2`.

  The goal of this task is to manipulate the producer options,
  if necessary at all, and introduce any new child specs that will be
  started in Broadway's supervision tree.
  """
  @callback prepare_to_connect(name :: atom, producer_opts :: keyword) ::
              {[:supervisor.child_spec() | {module, any} | module], producer_opts :: keyword}

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, message :: binary}

  @callback receive_messages(demand :: pos_integer, opts :: any) :: messages

  @optional_callbacks prepare_to_connect: 2
end
