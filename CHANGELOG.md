# CHANGELOG
All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). See the [CONTRIBUTING guide](./CONTRIBUTING.md#Changelog) for instructions on how to add changelog entries.

## [Unreleased 2.x]
### Add TokenManager Interface ([#7452](https://github.com/opensearch-project/OpenSearch/pull/7452))
=======
### Added
- [Extensions] Moving Extensions APIs to support cross versions via protobuf. ([#7402](https://github.com/opensearch-project/OpenSearch/issues/7402))
- [Extensions] Add IdentityPlugin into core to support Extension identities ([#7246](https://github.com/opensearch-project/OpenSearch/pull/7246))
- Add connectToNodeAsExtension in TransportService ([#6866](https://github.com/opensearch-project/OpenSearch/pull/6866))
- [Search Pipelines] Accept pipelines defined in search source ([#7253](https://github.com/opensearch-project/OpenSearch/pull/7253))
- [Search Pipelines] Add `default_search_pipeline` index setting ([#7470](https://github.com/opensearch-project/OpenSearch/pull/7470))
- [Search Pipelines] Add RenameFieldResponseProcessor for Search Pipelines ([#7377](https://github.com/opensearch-project/OpenSearch/pull/7377))
- [Search Pipelines] Split search pipeline processor factories by type ([#7597](https://github.com/opensearch-project/OpenSearch/pull/7597))
- [Search Pipelines] Add script processor ([#7607](https://github.com/opensearch-project/OpenSearch/pull/7607))
- Add descending order search optimization through reverse segment read. ([#7244](https://github.com/opensearch-project/OpenSearch/pull/7244))
- Add 'unsigned_long' numeric field type ([#6237](https://github.com/opensearch-project/OpenSearch/pull/6237))
- Add back primary shard preference for queries ([#7375](https://github.com/opensearch-project/OpenSearch/pull/7375))
- Add task cancellation timestamp in task API ([#7455](https://github.com/opensearch-project/OpenSearch/pull/7455))
- Adds ExtensionsManager.lookupExtensionSettingsById ([#7466](https://github.com/opensearch-project/OpenSearch/pull/7466))
- SegRep with Remote: Add hook for publishing checkpoint notifications after segment upload to remote store ([#7394](https://github.com/opensearch-project/OpenSearch/pull/7394))
- Add search_after query optimizations with shard/segment short cutting ([#7453](https://github.com/opensearch-project/OpenSearch/pull/7453))
- Provide mechanism to configure XContent parsing constraints (after update to Jackson 2.15.0 and above) ([#7550](https://github.com/opensearch-project/OpenSearch/pull/7550))
- Support to clear filecache using clear indices cache API ([#7498](https://github.com/opensearch-project/OpenSearch/pull/7498))
- Create NamedRoute to map extension routes to a shortened name ([#6870](https://github.com/opensearch-project/OpenSearch/pull/6870))
- Added @dbwiddis as on OpenSearch maintainer ([#7665](https://github.com/opensearch-project/OpenSearch/pull/7665))
- [Extensions] Add ExtensionAwarePlugin extension point to add custom settings for extensions ([#7526](https://github.com/opensearch-project/OpenSearch/pull/7526))
- Add new cluster setting to set default index replication type ([#7420](https://github.com/opensearch-project/OpenSearch/pull/7420))
- [Segment Replication] Rolling upgrade support for default codecs ([#7698](https://github.com/opensearch-project/OpenSearch/pull/7698))

### Dependencies
- Bump `com.azure:azure-storage-common` from 12.21.0 to 12.21.1 (#7566, #7814)
- Bump `com.google.guava:guava` from 30.1.1-jre to 32.0.0-jre (#7565, #7811, #7807, #7808)
- Bump `net.minidev:json-smart` from 2.4.10 to 2.4.11 (#7660, #7812)
- Bump `org.gradle.test-retry` from 1.5.2 to 1.5.3 (#7810)
- Bump `com.diffplug.spotless` from 6.17.0 to 6.18.0 (#7896)
- Bump `jackson` from 2.15.1 to 2.15.2 ([#7897](https://github.com/opensearch-project/OpenSearch/pull/7897))
- Add `com.github.luben:zstd-jni` version 1.5.5-3 ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Bump `netty` from 4.1.91.Final to 4.1.93.Final ([#7901](https://github.com/opensearch-project/OpenSearch/pull/7901))
- Bump `com.amazonaws` 1.12.270 to `software.amazon.awssdk` 2.20.55 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Add `org.reactivestreams` 1.0.4 ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))

### Changed
- Replace jboss-annotations-api_1.2_spec with jakarta.annotation-api ([#7836](https://github.com/opensearch-project/OpenSearch/pull/7836))
- Reduce memory copy in zstd compression ([#7681](https://github.com/opensearch-project/OpenSearch/pull/7681))
- Add min, max, average and thread info to resource stats in tasks API ([#7673](https://github.com/opensearch-project/OpenSearch/pull/7673))
- Add ZSTD compression for snapshotting ([#2996](https://github.com/opensearch-project/OpenSearch/pull/2996))
- Change `com.amazonaws.sdk.ec2MetadataServiceEndpointOverride` to `aws.ec2MetadataServiceEndpoint` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))
- Change `com.amazonaws.sdk.stsEndpointOverride` to `aws.stsEndpointOverride` ([7372](https://github.com/opensearch-project/OpenSearch/pull/7372/))

### Deprecated

### Removed

### Fixed
-  Fixing error: adding a new/forgotten parameter to the configuration for checking the config on startup in plugins/repository-s3 #7924

### Security

[Unreleased 2.x]: https://github.com/opensearch-project/OpenSearch/compare/2.8...2.x
