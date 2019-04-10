defmodule BroadwayCloudPubSub.Token do
  @moduledoc """
  A generic behaviour to get an access token for Google Cloud Pub/Sub.

  This module defines callbacks to get an access token for Google Cloud Pub/Sub.
  Modules that implement this behaviour should be passed as the `:token_provider`
  option from `BroadwayCloudPubSub.Producer`.
  """

  @callback token(scope :: String.t()) :: {:ok, String.t()} | :error
end
