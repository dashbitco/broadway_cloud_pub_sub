defmodule BroadwayCloudPubSub.RestClient do
  @moduledoc """
  A generic behaviour to implement Pub/Sub Clients for `BroadwayCloudPubSub.Producer` using the [REST API](https://cloud.google.com/pubsub/docs/reference/rest/).

  This module defines callbacks to normalize options and receive message
  from a Cloud Pub/Sub topic. Modules that implement this behaviour should be passed
  as the `:rest_client` option from `BroadwayCloudPubSub.Producer`.
  """

  alias Broadway.Message

  @type messages :: [Message.t()]

  @callback init(opts :: any) :: {:ok, normalized_opts :: any} | {:error, message :: binary}
  @callback receive_messages(demand :: pos_integer, opts :: any) :: messages
end
