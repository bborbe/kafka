package kafka

import "github.com/IBM/sarama"

//counterfeiter:generate -o mocks/kafka-sarama-consumer.go --fake-name KafkaSaramaConsumer . SaramaConsumer
type SaramaConsumer interface {
	sarama.Consumer
}
