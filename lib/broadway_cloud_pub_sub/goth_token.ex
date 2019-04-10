if Code.ensure_compiled?(Goth) do
  defmodule BroadwayCloudPubSub.GothToken do
    @moduledoc """
    Default token module used by `BroadwayCloudPubSub.Producer` to authenticate with Google.

    Authentication is handled by the `Goth` library, which must be explicitly included
    in your dependencies and configured for your application.
    """

    @behaviour BroadwayCloudPubSub.Token

    @impl true
    def token(scope) do
      with {:ok, %{token: token}} <- Goth.Token.for_scope(scope) do
        {:ok, token}
      end
    end
  end
end
