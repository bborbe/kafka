# Changelog

All notable changes to this project will be documented in this file.

Please choose versions by [Semantic Versioning](http://semver.org/).

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards-compatible manner, and
* PATCH version when you make backwards-compatible bug fixes.

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
