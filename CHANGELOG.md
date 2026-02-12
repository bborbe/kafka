# Changelog

All notable changes to this project will be documented in this file.

Please choose versions by [Semantic Versioning](http://semver.org/).

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards-compatible manner, and
* PATCH version when you make backwards-compatible bug fixes.

## Unreleased

- Update Go to 1.26.0

## v1.22.2

- Update Go to 1.25.7
- Update bborbe/* dependencies to latest patch versions
- Update testing dependencies (ginkgo v2.28.1, gomega v1.39.1)
- Update golang.org/x/* packages and other indirect dependencies

## v1.22.1

- Update Go to 1.25.5
- Update golang.org/x/crypto to v0.47.0
- Update dependencies

## v1.22.0

- Add `SaramaClientPool` interface with Acquire/Release pattern for connection pooling
- Add `SaramaClientProviderTypePool` provider type with automatic health checks
- Add `DefaultSaramaClientPoolOptions` for default pool configuration
- Add `reusedClient` wrapper to prevent accidental Close() of shared client
- Fix context propagation in pool health checks (accept ctx instead of Background())
- Fix race condition in pool provider with eager initialization
- Add comprehensive test coverage for pool implementation

## v1.21.0

- Add `ResetOffset` method to `OffsetManager` interface for backward offset movement
- Implement `ResetOffset` in all offset manager implementations (sarama, simple, store)
- Update offset manager HTTP handler to call `ResetOffset` before `MarkOffset`
- Add comprehensive GoDoc comments to `OffsetManager` interface methods
- Add counterfeiter mock generation for `OffsetManager` interface
- Add comprehensive test coverage for offset managers (simple, store, sarama, handler)
- Update dependencies (osv-scanner, gosec, anthropic-sdk-go, and others)

## v1.20.1

- Fix type compatibility: change map[string]string to map[string]any in error data

## v1.20.0

- update go and deps

## v1.19.3

- Add `DefaultOffsetStoreBucket` constant for offset store bucket name
- Pre-allocate slices in consumer and partition offset methods for better performance
- Add nolint directives for deprecated `ClosedError` alias and complex consumer logic
- Exclude unparam linter for test files in golangci-lint configuration
- Update Go version to 1.25.4
- Update dependencies (collection, http, run, containerd, sentry-go, and others)
- Improve linter configuration with additional rules and settings

## v1.19.2

- Remove configuration options parameter from `SaramaClientProvider.Client()` method
- Configuration options now set once during provider creation for cleaner API
- Update GoDoc comments to reflect simplified API

## v1.19.1

- Rename `ClosedError` to `ErrClosed` following Go naming conventions (ST1012)
- Add deprecated alias `ClosedError` for backward compatibility
- Update internal usage to use `ErrClosed`

## v1.19.0

- Add `NewSaramaClientProviderExisting` adapter to wrap existing Sarama clients with provider pattern
- Add `NewOffsetConsumerWithProvider` for provider-based offset consumer creation
- Add `NewOffsetConsumerBatchWithProvider` for provider-based batch offset consumer creation
- Add `NewOffsetConsumerHighwaterMarksWithProvider` for provider-based highwater marks consumer creation
- Add `NewOffsetConsumerHighwaterMarksBatchWithProvider` for provider-based batch highwater marks consumer creation
- Add `NewSyncProducerFromSaramaClient` to create producer from existing client
- Add `NewSyncProducerFromSaramaClientProvider` for provider-based producer creation
- Refactor existing consumer constructors to use adapter pattern internally (zero breaking changes)
- Fix missing parameter initialization bug in `NewSaramaClientProviderExisting`
- Add comprehensive GoDoc comments for all new exported functions

## v1.18.0

- Add SaramaClientProvider interface for flexible Sarama client lifecycle management
- Add NewSaramaClientProvider for creating new clients per consumer
- Add ReusedSaramaClientProvider for sharing single client across consumers
- Add comprehensive test coverage for client provider implementations

## v1.17.5

- Fix naming convention for JSON-related types and functions (JsonSender → JSONSender, NewJsonEncoder → NewJSONEncoder)
- Add deprecated aliases for backward compatibility (JsonSender, JsonSenderOptions, NewJsonEncoder)
- Improve error handling in update handler functions
- Add comprehensive test coverage for message handler functions
- Configure golangci-lint to exclude duplicate code warnings in test files

## v1.17.4

- Restore encoding.TextMarshaler support for Broker type
- Restore encoding.TextMarshaler support for Brokers type
- Restore comprehensive test coverage for MarshalText methods

## v1.17.2

- Add encoding.TextMarshaler support for Broker type
- Add encoding.TextMarshaler support for Brokers type
- Add comprehensive test coverage for MarshalText methods

## v1.17.1

- Remove deprecated golang.org/x/lint/golint from tools.go
- Fix README code examples to match actual API signatures
- Update godoc.org badge to pkg.go.dev
- Update Go version from 1.25.2 to 1.25.3
- Add CI badge to README
- Add table of contents to README for better navigation

## v1.17.0

- Add encoding.TextUnmarshaler support for Broker type
- Add encoding.TextUnmarshaler support for Brokers type
- Add comprehensive test coverage for UnmarshalText methods

## v1.16.0

- Add NewGzipEncoder for gzip compression with default compression level
- Add NewGzipEncoderWithLevel for gzip compression with configurable compression levels
- Add GzipDecoder for gzip decompression
- Add comprehensive test coverage for gzip encoder and decoder

## v1.15.0

- Add golangci-lint configuration with multiple linters enabled
- Add tools.go for Go development tool dependency management
- Enhanced Makefile with new security and quality checks (osv-scanner, gosec, trivy)
- Apply consistent code formatting with golines (max line length 100)
- Update development dependencies in go.mod

## v1.14.1

- revert JSONSender back to JsonSender

## v1.14.0

- Add IsBrokenPipeError function for detecting broken pipe network errors
- Add LICENSE file with BSD license
- Update GitHub workflows and CI configuration
- Fix test coverage reporting
- Improve counterfeiter generate comment placement

## v1.13.7

- add tests
- go mod update

## v1.13.6

- log error message in ErrorHandler

## v1.13.5

- change loglevel

## v1.13.4

- add BatchSize.Int64
- go mod update

## v1.13.3

- use mod=mod

## v1.13.2

- add consumer error handler

## v1.13.1

- add topics sort

## v1.13.0

- add SimpleConsumerBatch

## v1.12.3

- add logging

## v1.12.2

- log ConsumerOptions on startup

## v1.12.1

- add ConsumerOptions to OffsetConsumerHighwaterMarks

## v1.12.0

- add ConsumerOptions to OffsetConsumer
- add ConsumerOptions to SimpleConsumer
- allow wait if lag is small

## v1.11.3

- fix NewMessageHandlerBatchDelay

## v1.11.2

- add MessageHandlerBatchDelay

## v1.11.1

- improve logging

## v1.11.0

- remove vendor
- go mod update

## v1.10.5

- add NewSimpleOffsetManager
- go mod update

## v1.10.4

- go mod update

## v1.10.3

- initialize failure_out_of_range_counter metrics
- go mod update

## v1.10.2

- add create partition consumer metrics
- fix syncProducerMetrics

## v1.10.1

- improve logging

## v1.10.0

- add fallbackOffset to OffsetManager, allows initalOffset to be oldest and fallback if outofrange latest

## v1.9.6

- allow close OffsetManager

## v1.9.5

- fix OffsetManagerHandler
- go mod update

## v1.9.4

- allow negative offset in offsetmanager

## v1.9.3

- add ParseTopicsFromString and ParseTopics
- go mod update

## v1.9.2

- SyncProducer connect to host without schema

## v1.9.1

- connect to host without schema

## v1.9.0

- allow JsonSender send to multi topic in one batch

## v1.8.0

- add Schema to Broker (plain + tls)
- add tls support to SaramaClient

## v1.7.2

- add TopicBucketFromStrings -> TopicFromStrings

## v1.7.1

- add TopicBucketFromStrings

## v1.7.0

- add Topic.Validate
- go mod update

## v1.6.8

- add topics

## v1.6.7

- add MessageHandlerBatchMetrics

## v1.6.6

- fix NewSyncProducerNop
- go mod update

## v1.6.5

- add SyncProducerNop
- go mod update

## v1.6.4

- update kafka version to 3.6.0
- go mod update

## v1.6.3

- add UpdateHandlerFilter
- go mod update

## v1.6.2

- flip [OBJECT,KEY] -> [KEY,OBJECT]
- add UpdateHandlerView and UpdateHandlerUpdate

## v1.6.1

- allow string|[]byte as Key

## v1.6.0

- add update handler

## v1.5.3

- fix syncProducer close
- add Sarama SyncProducer mock

## v1.5.2

- offsetStore only returns initial offset on bucket or key not found errors

## v1.5.1

- add SyncProducerWithHeader
- add SyncProducerWithName

## v1.5.0

- add context to SyncProducer send
- add SyncProducerModify
- go mod update

## v1.4.0

- go mod update
- add metrics batch message handler

## v1.3.0

- add syncProducer metrics

## v1.2.2

- rename metrics messageHandler

## v1.2.1

- add messageHandler metrics

## v1.2.0

- add consumer metrics

## v1.1.1

- Fix BatchSize.Int()
- Add BatchSize.Validate()

## v1.1.0

- Add BatchSize type

## v1.0.1

- Fix mocks

## v1.0.0

- Initial Version
