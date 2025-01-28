package kafka

import "github.com/IBM/sarama"

//counterfeiter:generate -o mocks/kafka-sarama-partition-consumer.go --fake-name KafkaSaramaPartitionConsumer . SaramaPartitionConsumer
type SaramaPartitionConsumer interface {
	sarama.PartitionConsumer
}
