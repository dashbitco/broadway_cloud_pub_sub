defmodule BroadwayCloudPubSub.DummyToken do
  @moduledoc """
  A token provider that generates fake tokens, used mostly for testing.
  """
  @behaviour BroadwayCloudPubSub.Token

  @name inspect(__MODULE__)

  def token(_), do: {:ok, "#{@name}.#{System.os_time(:second)}"}
end
