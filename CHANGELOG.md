# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0]

### Added

- Client options for connection pools (#37)

- Support for configuring acknowledgement behavior (#36)

### Changed

- Move acknowledger behaviour from `GoogleApiClient` into `ClientAcknowledger` (#39)

## [0.4.0] - 2019-08-19

### Changed

- Move to Plataformatec GitHub organization and become an official Broadway connector

- Rename behaviour `RestClient` to `Client` (#23)

- Use hackney as the default adapter (#20)

- Require Broadway 0.4.x and (optionally) Goth 1.x (#26)

- Replace `:token_module` option with `:token_generator` (#29)

- Hide `handle_receive_messages` function that was accidentally made public

## [0.3.0] - 2019-05-08

### Changed
- **BREAKING:** The PubsubMessage struct now gets unpacked into the `%Broadway.Message{}` received in your pipeline.  If you were using `message.data.data` before, you can now use `message.data`. Additional properties from the PubsubMessage can be found in the message metadata, for instance: `message.metadata.attributes` or `message.metadata.messageId`.
- Requires `:broadway ~> 0.3.0`

## [0.1.3] - 2019-05-06

### Changed
- Fixed `BroadwayCloudPubSub.GoogleApiClient` attempting to send an empty acknowledge request.

## [0.1.2] - 2019-04-11

### Changed
- MIX_ENV for publishing releases to Hex.

## [0.1.1] - 2019-04-11
### Added
- This `CHANGELOG` file to hopefully serve as an evolving example of a
  standardized open source project `CHANGELOG`.

### Changed
- Fixed CircleCI build for publishing docs to Hex.

## [0.1.0] - 2019-04-10
### Added
- `BroadwayCloudPubSub.Producer` - A GenStage producer that continuously receives messages from
    a Pub/Sub subscription acknowledges them after being successfully processed.
- `BroadwayCloudPubSub.RestClient` - A generic behaviour to implement Pub/Sub clients using the [REST API](https://cloud.google.com/pubsub/docs/reference/rest/).
- `BroadwayCloudPubSub.GoogleApiClient` - Default REST client used by `BroadwayCloudPubSub.Producer`.
- `BroadwayCloudPubSub.Token` - A generic behaviour to implement token authentication for Pub/Sub clients.
- `BroadwayCloudPubSub.GothToken` - Default token provider used by `BroadwayCloudPubSub.Producer`.


[Unreleased]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.1.3...v0.3.0
[0.1.3]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/plataformatec/broadway_cloud_pub_sub/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/plataformatec/broadway_cloud_pub_sub/releases/tag/v0.1.0
