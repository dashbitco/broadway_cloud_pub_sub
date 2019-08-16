# BroadwayCloudPubSub

[![Build Status](https://travis-ci.org/plataformatec/broadway_cloud_pub_sub.svg?branch=master)](https://travis-ci.org/plataformatec/broadway_cloud_pub_sub)

A Google Cloud Pub/Sub connector for [Broadway](https://github.com/plataformatec/broadway).

Documentation can be found at [https://hexdocs.pm/broadway_cloud_pub_sub](https://hexdocs.pm/broadway_cloud_pub_sub).

This project provides:

  * `BroadwayCloudPubSub.Producer` - A GenStage producer that continuously receives messages from
    a Pub/Sub subscription acknowledges them after being successfully processed.
  * `BroadwayCloudPubSub.Client` - A generic behaviour to implement Pub/Sub clients.
  * `BroadwayCloudPubSub.GoogleApiClient` - Default REST client used by `BroadwayCloudPubSub.Producer`.
  * `BroadwayCloudPubSub.Token` - A generic behaviour to implement token authentication for Pub/Sub clients.
  * `BroadwayCloudPubSub.GothToken` - Default token provider used by `BroadwayCloudPubSub.Producer`.
  * `BroadwayCloudPubSub.DummyToken` - A token provider that generates fake tokens, used mostly for testing.


## Installation

Add `:broadway_cloud_pub_sub` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:broadway_cloud_pub_sub, "~> 0.3.0"},
    {:goth, "~> 0.11"}
  ]
end
```
> Note the [goth](https://hexdocs.pm/goth) package, which handles Google Authentication, is required for the default token implementation.

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

## License

Copyright 2019 Plataformatec

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
