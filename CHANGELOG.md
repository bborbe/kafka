# Changelog

All notable changes to this project will be documented in this file.

Please choose versions by [Semantic Versioning](http://semver.org/).

* MAJOR version when you make incompatible API changes,
* MINOR version when you add functionality in a backwards-compatible manner, and
* PATCH version when you make backwards-compatible bug fixes.

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
