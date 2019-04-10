defmodule BroadwayCloudPubSub.FakeToken do
  @behaviour BroadwayCloudPubSub.Token

  def token(_), do: {:ok, UUID.uuid1()}
end
