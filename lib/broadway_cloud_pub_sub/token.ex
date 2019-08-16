defmodule BroadwayCloudPubSub.Token do
  @moduledoc false

  @callback token(scope :: String.t()) :: {:ok, String.t()} | :error
end
