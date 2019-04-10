use Mix.Config

# Disable Goth in tests
config :goth, disabled: true

# Use mock HTTP client in tests
config :tesla, GoogleApi.PubSub.V1.Connection, adapter: Tesla.Mock
