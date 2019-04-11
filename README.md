# BroadwayCloudPubSub

A Google Cloud Pub/Sub connector for [Broadway](https://github.com/plataformatec/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_cloud_pub_sub](https://hexdocs.pm/broadway_cloud_pub_sub).

This project provides:

  * `BroadwayCloudPubSub.Producer` - A GenStage producer that continuously receives messages from
    a Pub/Sub subscription acknowledges them after being successfully processed.
  * `BroadwayCloudPubSub.RestClient` - A generic behaviour to implement Pub/Sub clients using the [REST API](https://cloud.google.com/pubsub/docs/reference/rest/).
  * `BroadwayCloudPubSub.GoogleApiClient` - Default REST client used by `BroadwayCloudPubSub.Producer`.
  * `BroadwayCloudPubSub.Token` - A generic behaviour to implement token authentication for Pub/Sub clients.
  * `BroadwayCloudPubSub.GothToken` - Default token provider used by `BroadwayCloudPubSub.Producer`.


## Installation

Add `:broadway_cloud_pub_sub` to the list of dependencies in `mix.exs`, along with the authentication
library of your choice (defaults to `:goth`):

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 0.1.0"},
    {:goth, "~> 0.11"}
  ]
end
```

By default, `:goth` will include `:hackney` as its HTTP client. If you choose to use an alternative
authentication library, you may need to explicitly include an HTTP client (such as `:hackney`):

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 0.1.0"},
    {:hackney, "~> 1.9"}
  ]
end
```

## Usage

Configure Broadway with one or more producers using `BroadwayCloudPubSub.Producer`:

```elixir
Broadway.start_link(
  MyBroadway,
  name: MyBroadway,
  producers: [
    default: [
      module:
        {BroadwayCloudPubSub.Producer,
         subscription: "projects/my-project/subscriptions/my-subscription"}
    ]
  ]
)
```

For more information, please see the docs for [Broadway](https://hexdocs.pm/broadway).

There is also an example app available at: [https://github.com/mcrumm/broadway_cloud_pub_sub](https://github.com/mcrumm/broadway_cloud_pub_sub)
