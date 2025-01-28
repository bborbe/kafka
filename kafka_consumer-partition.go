package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/bborbe/errors"
	"github.com/golang/glog"
	"strings"
)

const OutOfRangeErrorMessage = "The requested offset is outside the range of offsets maintained by the server for the given topic/partition"

// CreatePartitionConsumer create partition consumer and use initial offset if out of range error
func CreatePartitionConsumer(
	ctx context.Context,
	consumerFromClient sarama.Consumer,
	topic Topic,
	partition Partition,
	fallbackOffset Offset, // offset to use if OutOfRangeErrorMessage
	nextOffset Offset,
) (sarama.PartitionConsumer, error) {
	consumePartition, err := consumerFromClient.ConsumePartition(topic.String(), partition.Int32(), nextOffset.Int64())
	if err != nil {
		if strings.Contains(err.Error(), OutOfRangeErrorMessage) {
			glog.Warningf("create partition consumer for topic(%s) and partition(%s) got out of range error => fallback to initial offset(%s)", topic, partition, fallbackOffset)
			return consumerFromClient.ConsumePartition(topic.String(), partition.Int32(), fallbackOffset.Int64())
		}
		return nil, errors.Wrapf(ctx, err, "create partition consumer failed")
	}
	return consumePartition, nil
}
