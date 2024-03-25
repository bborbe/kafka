// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"
	"time"

	"github.com/bborbe/kafka"
)

type KafkaMetrics struct {
	CurrentOffsetStub        func(kafka.Topic, kafka.Partition, kafka.Offset)
	currentOffsetMutex       sync.RWMutex
	currentOffsetArgsForCall []struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
		arg3 kafka.Offset
	}
	DurationMeasureStub        func(kafka.Topic, kafka.Partition, time.Duration)
	durationMeasureMutex       sync.RWMutex
	durationMeasureArgsForCall []struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
		arg3 time.Duration
	}
	FailureCounterIncStub        func(kafka.Topic, kafka.Partition)
	failureCounterIncMutex       sync.RWMutex
	failureCounterIncArgsForCall []struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
	}
	HighWaterMarkOffsetStub        func(kafka.Topic, kafka.Partition, kafka.Offset)
	highWaterMarkOffsetMutex       sync.RWMutex
	highWaterMarkOffsetArgsForCall []struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
		arg3 kafka.Offset
	}
	SuccessCounterIncStub        func(kafka.Topic, kafka.Partition)
	successCounterIncMutex       sync.RWMutex
	successCounterIncArgsForCall []struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
	}
	TotalCounterIncStub        func(kafka.Topic, kafka.Partition)
	totalCounterIncMutex       sync.RWMutex
	totalCounterIncArgsForCall []struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *KafkaMetrics) CurrentOffset(arg1 kafka.Topic, arg2 kafka.Partition, arg3 kafka.Offset) {
	fake.currentOffsetMutex.Lock()
	fake.currentOffsetArgsForCall = append(fake.currentOffsetArgsForCall, struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
		arg3 kafka.Offset
	}{arg1, arg2, arg3})
	stub := fake.CurrentOffsetStub
	fake.recordInvocation("CurrentOffset", []interface{}{arg1, arg2, arg3})
	fake.currentOffsetMutex.Unlock()
	if stub != nil {
		fake.CurrentOffsetStub(arg1, arg2, arg3)
	}
}

func (fake *KafkaMetrics) CurrentOffsetCallCount() int {
	fake.currentOffsetMutex.RLock()
	defer fake.currentOffsetMutex.RUnlock()
	return len(fake.currentOffsetArgsForCall)
}

func (fake *KafkaMetrics) CurrentOffsetCalls(stub func(kafka.Topic, kafka.Partition, kafka.Offset)) {
	fake.currentOffsetMutex.Lock()
	defer fake.currentOffsetMutex.Unlock()
	fake.CurrentOffsetStub = stub
}

func (fake *KafkaMetrics) CurrentOffsetArgsForCall(i int) (kafka.Topic, kafka.Partition, kafka.Offset) {
	fake.currentOffsetMutex.RLock()
	defer fake.currentOffsetMutex.RUnlock()
	argsForCall := fake.currentOffsetArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *KafkaMetrics) DurationMeasure(arg1 kafka.Topic, arg2 kafka.Partition, arg3 time.Duration) {
	fake.durationMeasureMutex.Lock()
	fake.durationMeasureArgsForCall = append(fake.durationMeasureArgsForCall, struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
		arg3 time.Duration
	}{arg1, arg2, arg3})
	stub := fake.DurationMeasureStub
	fake.recordInvocation("DurationMeasure", []interface{}{arg1, arg2, arg3})
	fake.durationMeasureMutex.Unlock()
	if stub != nil {
		fake.DurationMeasureStub(arg1, arg2, arg3)
	}
}

func (fake *KafkaMetrics) DurationMeasureCallCount() int {
	fake.durationMeasureMutex.RLock()
	defer fake.durationMeasureMutex.RUnlock()
	return len(fake.durationMeasureArgsForCall)
}

func (fake *KafkaMetrics) DurationMeasureCalls(stub func(kafka.Topic, kafka.Partition, time.Duration)) {
	fake.durationMeasureMutex.Lock()
	defer fake.durationMeasureMutex.Unlock()
	fake.DurationMeasureStub = stub
}

func (fake *KafkaMetrics) DurationMeasureArgsForCall(i int) (kafka.Topic, kafka.Partition, time.Duration) {
	fake.durationMeasureMutex.RLock()
	defer fake.durationMeasureMutex.RUnlock()
	argsForCall := fake.durationMeasureArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *KafkaMetrics) FailureCounterInc(arg1 kafka.Topic, arg2 kafka.Partition) {
	fake.failureCounterIncMutex.Lock()
	fake.failureCounterIncArgsForCall = append(fake.failureCounterIncArgsForCall, struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
	}{arg1, arg2})
	stub := fake.FailureCounterIncStub
	fake.recordInvocation("FailureCounterInc", []interface{}{arg1, arg2})
	fake.failureCounterIncMutex.Unlock()
	if stub != nil {
		fake.FailureCounterIncStub(arg1, arg2)
	}
}

func (fake *KafkaMetrics) FailureCounterIncCallCount() int {
	fake.failureCounterIncMutex.RLock()
	defer fake.failureCounterIncMutex.RUnlock()
	return len(fake.failureCounterIncArgsForCall)
}

func (fake *KafkaMetrics) FailureCounterIncCalls(stub func(kafka.Topic, kafka.Partition)) {
	fake.failureCounterIncMutex.Lock()
	defer fake.failureCounterIncMutex.Unlock()
	fake.FailureCounterIncStub = stub
}

func (fake *KafkaMetrics) FailureCounterIncArgsForCall(i int) (kafka.Topic, kafka.Partition) {
	fake.failureCounterIncMutex.RLock()
	defer fake.failureCounterIncMutex.RUnlock()
	argsForCall := fake.failureCounterIncArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *KafkaMetrics) HighWaterMarkOffset(arg1 kafka.Topic, arg2 kafka.Partition, arg3 kafka.Offset) {
	fake.highWaterMarkOffsetMutex.Lock()
	fake.highWaterMarkOffsetArgsForCall = append(fake.highWaterMarkOffsetArgsForCall, struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
		arg3 kafka.Offset
	}{arg1, arg2, arg3})
	stub := fake.HighWaterMarkOffsetStub
	fake.recordInvocation("HighWaterMarkOffset", []interface{}{arg1, arg2, arg3})
	fake.highWaterMarkOffsetMutex.Unlock()
	if stub != nil {
		fake.HighWaterMarkOffsetStub(arg1, arg2, arg3)
	}
}

func (fake *KafkaMetrics) HighWaterMarkOffsetCallCount() int {
	fake.highWaterMarkOffsetMutex.RLock()
	defer fake.highWaterMarkOffsetMutex.RUnlock()
	return len(fake.highWaterMarkOffsetArgsForCall)
}

func (fake *KafkaMetrics) HighWaterMarkOffsetCalls(stub func(kafka.Topic, kafka.Partition, kafka.Offset)) {
	fake.highWaterMarkOffsetMutex.Lock()
	defer fake.highWaterMarkOffsetMutex.Unlock()
	fake.HighWaterMarkOffsetStub = stub
}

func (fake *KafkaMetrics) HighWaterMarkOffsetArgsForCall(i int) (kafka.Topic, kafka.Partition, kafka.Offset) {
	fake.highWaterMarkOffsetMutex.RLock()
	defer fake.highWaterMarkOffsetMutex.RUnlock()
	argsForCall := fake.highWaterMarkOffsetArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *KafkaMetrics) SuccessCounterInc(arg1 kafka.Topic, arg2 kafka.Partition) {
	fake.successCounterIncMutex.Lock()
	fake.successCounterIncArgsForCall = append(fake.successCounterIncArgsForCall, struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
	}{arg1, arg2})
	stub := fake.SuccessCounterIncStub
	fake.recordInvocation("SuccessCounterInc", []interface{}{arg1, arg2})
	fake.successCounterIncMutex.Unlock()
	if stub != nil {
		fake.SuccessCounterIncStub(arg1, arg2)
	}
}

func (fake *KafkaMetrics) SuccessCounterIncCallCount() int {
	fake.successCounterIncMutex.RLock()
	defer fake.successCounterIncMutex.RUnlock()
	return len(fake.successCounterIncArgsForCall)
}

func (fake *KafkaMetrics) SuccessCounterIncCalls(stub func(kafka.Topic, kafka.Partition)) {
	fake.successCounterIncMutex.Lock()
	defer fake.successCounterIncMutex.Unlock()
	fake.SuccessCounterIncStub = stub
}

func (fake *KafkaMetrics) SuccessCounterIncArgsForCall(i int) (kafka.Topic, kafka.Partition) {
	fake.successCounterIncMutex.RLock()
	defer fake.successCounterIncMutex.RUnlock()
	argsForCall := fake.successCounterIncArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *KafkaMetrics) TotalCounterInc(arg1 kafka.Topic, arg2 kafka.Partition) {
	fake.totalCounterIncMutex.Lock()
	fake.totalCounterIncArgsForCall = append(fake.totalCounterIncArgsForCall, struct {
		arg1 kafka.Topic
		arg2 kafka.Partition
	}{arg1, arg2})
	stub := fake.TotalCounterIncStub
	fake.recordInvocation("TotalCounterInc", []interface{}{arg1, arg2})
	fake.totalCounterIncMutex.Unlock()
	if stub != nil {
		fake.TotalCounterIncStub(arg1, arg2)
	}
}

func (fake *KafkaMetrics) TotalCounterIncCallCount() int {
	fake.totalCounterIncMutex.RLock()
	defer fake.totalCounterIncMutex.RUnlock()
	return len(fake.totalCounterIncArgsForCall)
}

func (fake *KafkaMetrics) TotalCounterIncCalls(stub func(kafka.Topic, kafka.Partition)) {
	fake.totalCounterIncMutex.Lock()
	defer fake.totalCounterIncMutex.Unlock()
	fake.TotalCounterIncStub = stub
}

func (fake *KafkaMetrics) TotalCounterIncArgsForCall(i int) (kafka.Topic, kafka.Partition) {
	fake.totalCounterIncMutex.RLock()
	defer fake.totalCounterIncMutex.RUnlock()
	argsForCall := fake.totalCounterIncArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *KafkaMetrics) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.currentOffsetMutex.RLock()
	defer fake.currentOffsetMutex.RUnlock()
	fake.durationMeasureMutex.RLock()
	defer fake.durationMeasureMutex.RUnlock()
	fake.failureCounterIncMutex.RLock()
	defer fake.failureCounterIncMutex.RUnlock()
	fake.highWaterMarkOffsetMutex.RLock()
	defer fake.highWaterMarkOffsetMutex.RUnlock()
	fake.successCounterIncMutex.RLock()
	defer fake.successCounterIncMutex.RUnlock()
	fake.totalCounterIncMutex.RLock()
	defer fake.totalCounterIncMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *KafkaMetrics) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ kafka.Metrics = new(KafkaMetrics)
