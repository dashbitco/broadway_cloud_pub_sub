if Code.ensure_compiled?(Goth) do
  defmodule BroadwayCloudPubSub.GothToken do
    @moduledoc false

    @behaviour BroadwayCloudPubSub.Token

    @impl true
    def token(scope) do
      with {:ok, %{token: token}} <- Goth.Token.for_scope(scope) do
        {:ok, token}
      end
    end
  end
end
