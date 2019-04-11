# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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


[Unreleased]: https://github.com/mcrumm/broadway_cloud_pub_sub/compare/v0.1.2...HEAD
[0.1.2]: https://github.com/mcrumm/broadway_cloud_pub_sub/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/mcrumm/broadway_cloud_pub_sub/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/mcrumm/broadway_cloud_pub_sub/releases/tag/v0.1.0
