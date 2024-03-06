// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
	"github.com/bborbe/kafka"
)

type KafkaJsonSender struct {
	SendDeleteStub        func(context.Context, kafka.Topic, kafka.Key, ...sarama.RecordHeader) error
	sendDeleteMutex       sync.RWMutex
	sendDeleteArgsForCall []struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Key
		arg4 []sarama.RecordHeader
	}
	sendDeleteReturns struct {
		result1 error
	}
	sendDeleteReturnsOnCall map[int]struct {
		result1 error
	}
	SendDeletesStub        func(context.Context, kafka.Topic, kafka.Entries) error
	sendDeletesMutex       sync.RWMutex
	sendDeletesArgsForCall []struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Entries
	}
	sendDeletesReturns struct {
		result1 error
	}
	sendDeletesReturnsOnCall map[int]struct {
		result1 error
	}
	SendUpdateStub        func(context.Context, kafka.Topic, kafka.Key, kafka.Value, ...sarama.RecordHeader) error
	sendUpdateMutex       sync.RWMutex
	sendUpdateArgsForCall []struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Key
		arg4 kafka.Value
		arg5 []sarama.RecordHeader
	}
	sendUpdateReturns struct {
		result1 error
	}
	sendUpdateReturnsOnCall map[int]struct {
		result1 error
	}
	SendUpdatesStub        func(context.Context, kafka.Topic, kafka.Entries) error
	sendUpdatesMutex       sync.RWMutex
	sendUpdatesArgsForCall []struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Entries
	}
	sendUpdatesReturns struct {
		result1 error
	}
	sendUpdatesReturnsOnCall map[int]struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *KafkaJsonSender) SendDelete(arg1 context.Context, arg2 kafka.Topic, arg3 kafka.Key, arg4 ...sarama.RecordHeader) error {
	fake.sendDeleteMutex.Lock()
	ret, specificReturn := fake.sendDeleteReturnsOnCall[len(fake.sendDeleteArgsForCall)]
	fake.sendDeleteArgsForCall = append(fake.sendDeleteArgsForCall, struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Key
		arg4 []sarama.RecordHeader
	}{arg1, arg2, arg3, arg4})
	stub := fake.SendDeleteStub
	fakeReturns := fake.sendDeleteReturns
	fake.recordInvocation("SendDelete", []interface{}{arg1, arg2, arg3, arg4})
	fake.sendDeleteMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3, arg4...)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaJsonSender) SendDeleteCallCount() int {
	fake.sendDeleteMutex.RLock()
	defer fake.sendDeleteMutex.RUnlock()
	return len(fake.sendDeleteArgsForCall)
}

func (fake *KafkaJsonSender) SendDeleteCalls(stub func(context.Context, kafka.Topic, kafka.Key, ...sarama.RecordHeader) error) {
	fake.sendDeleteMutex.Lock()
	defer fake.sendDeleteMutex.Unlock()
	fake.SendDeleteStub = stub
}

func (fake *KafkaJsonSender) SendDeleteArgsForCall(i int) (context.Context, kafka.Topic, kafka.Key, []sarama.RecordHeader) {
	fake.sendDeleteMutex.RLock()
	defer fake.sendDeleteMutex.RUnlock()
	argsForCall := fake.sendDeleteArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3, argsForCall.arg4
}

func (fake *KafkaJsonSender) SendDeleteReturns(result1 error) {
	fake.sendDeleteMutex.Lock()
	defer fake.sendDeleteMutex.Unlock()
	fake.SendDeleteStub = nil
	fake.sendDeleteReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendDeleteReturnsOnCall(i int, result1 error) {
	fake.sendDeleteMutex.Lock()
	defer fake.sendDeleteMutex.Unlock()
	fake.SendDeleteStub = nil
	if fake.sendDeleteReturnsOnCall == nil {
		fake.sendDeleteReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.sendDeleteReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendDeletes(arg1 context.Context, arg2 kafka.Topic, arg3 kafka.Entries) error {
	fake.sendDeletesMutex.Lock()
	ret, specificReturn := fake.sendDeletesReturnsOnCall[len(fake.sendDeletesArgsForCall)]
	fake.sendDeletesArgsForCall = append(fake.sendDeletesArgsForCall, struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Entries
	}{arg1, arg2, arg3})
	stub := fake.SendDeletesStub
	fakeReturns := fake.sendDeletesReturns
	fake.recordInvocation("SendDeletes", []interface{}{arg1, arg2, arg3})
	fake.sendDeletesMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaJsonSender) SendDeletesCallCount() int {
	fake.sendDeletesMutex.RLock()
	defer fake.sendDeletesMutex.RUnlock()
	return len(fake.sendDeletesArgsForCall)
}

func (fake *KafkaJsonSender) SendDeletesCalls(stub func(context.Context, kafka.Topic, kafka.Entries) error) {
	fake.sendDeletesMutex.Lock()
	defer fake.sendDeletesMutex.Unlock()
	fake.SendDeletesStub = stub
}

func (fake *KafkaJsonSender) SendDeletesArgsForCall(i int) (context.Context, kafka.Topic, kafka.Entries) {
	fake.sendDeletesMutex.RLock()
	defer fake.sendDeletesMutex.RUnlock()
	argsForCall := fake.sendDeletesArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *KafkaJsonSender) SendDeletesReturns(result1 error) {
	fake.sendDeletesMutex.Lock()
	defer fake.sendDeletesMutex.Unlock()
	fake.SendDeletesStub = nil
	fake.sendDeletesReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendDeletesReturnsOnCall(i int, result1 error) {
	fake.sendDeletesMutex.Lock()
	defer fake.sendDeletesMutex.Unlock()
	fake.SendDeletesStub = nil
	if fake.sendDeletesReturnsOnCall == nil {
		fake.sendDeletesReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.sendDeletesReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendUpdate(arg1 context.Context, arg2 kafka.Topic, arg3 kafka.Key, arg4 kafka.Value, arg5 ...sarama.RecordHeader) error {
	fake.sendUpdateMutex.Lock()
	ret, specificReturn := fake.sendUpdateReturnsOnCall[len(fake.sendUpdateArgsForCall)]
	fake.sendUpdateArgsForCall = append(fake.sendUpdateArgsForCall, struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Key
		arg4 kafka.Value
		arg5 []sarama.RecordHeader
	}{arg1, arg2, arg3, arg4, arg5})
	stub := fake.SendUpdateStub
	fakeReturns := fake.sendUpdateReturns
	fake.recordInvocation("SendUpdate", []interface{}{arg1, arg2, arg3, arg4, arg5})
	fake.sendUpdateMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3, arg4, arg5...)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaJsonSender) SendUpdateCallCount() int {
	fake.sendUpdateMutex.RLock()
	defer fake.sendUpdateMutex.RUnlock()
	return len(fake.sendUpdateArgsForCall)
}

func (fake *KafkaJsonSender) SendUpdateCalls(stub func(context.Context, kafka.Topic, kafka.Key, kafka.Value, ...sarama.RecordHeader) error) {
	fake.sendUpdateMutex.Lock()
	defer fake.sendUpdateMutex.Unlock()
	fake.SendUpdateStub = stub
}

func (fake *KafkaJsonSender) SendUpdateArgsForCall(i int) (context.Context, kafka.Topic, kafka.Key, kafka.Value, []sarama.RecordHeader) {
	fake.sendUpdateMutex.RLock()
	defer fake.sendUpdateMutex.RUnlock()
	argsForCall := fake.sendUpdateArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3, argsForCall.arg4, argsForCall.arg5
}

func (fake *KafkaJsonSender) SendUpdateReturns(result1 error) {
	fake.sendUpdateMutex.Lock()
	defer fake.sendUpdateMutex.Unlock()
	fake.SendUpdateStub = nil
	fake.sendUpdateReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendUpdateReturnsOnCall(i int, result1 error) {
	fake.sendUpdateMutex.Lock()
	defer fake.sendUpdateMutex.Unlock()
	fake.SendUpdateStub = nil
	if fake.sendUpdateReturnsOnCall == nil {
		fake.sendUpdateReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.sendUpdateReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendUpdates(arg1 context.Context, arg2 kafka.Topic, arg3 kafka.Entries) error {
	fake.sendUpdatesMutex.Lock()
	ret, specificReturn := fake.sendUpdatesReturnsOnCall[len(fake.sendUpdatesArgsForCall)]
	fake.sendUpdatesArgsForCall = append(fake.sendUpdatesArgsForCall, struct {
		arg1 context.Context
		arg2 kafka.Topic
		arg3 kafka.Entries
	}{arg1, arg2, arg3})
	stub := fake.SendUpdatesStub
	fakeReturns := fake.sendUpdatesReturns
	fake.recordInvocation("SendUpdates", []interface{}{arg1, arg2, arg3})
	fake.sendUpdatesMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaJsonSender) SendUpdatesCallCount() int {
	fake.sendUpdatesMutex.RLock()
	defer fake.sendUpdatesMutex.RUnlock()
	return len(fake.sendUpdatesArgsForCall)
}

func (fake *KafkaJsonSender) SendUpdatesCalls(stub func(context.Context, kafka.Topic, kafka.Entries) error) {
	fake.sendUpdatesMutex.Lock()
	defer fake.sendUpdatesMutex.Unlock()
	fake.SendUpdatesStub = stub
}

func (fake *KafkaJsonSender) SendUpdatesArgsForCall(i int) (context.Context, kafka.Topic, kafka.Entries) {
	fake.sendUpdatesMutex.RLock()
	defer fake.sendUpdatesMutex.RUnlock()
	argsForCall := fake.sendUpdatesArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *KafkaJsonSender) SendUpdatesReturns(result1 error) {
	fake.sendUpdatesMutex.Lock()
	defer fake.sendUpdatesMutex.Unlock()
	fake.SendUpdatesStub = nil
	fake.sendUpdatesReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) SendUpdatesReturnsOnCall(i int, result1 error) {
	fake.sendUpdatesMutex.Lock()
	defer fake.sendUpdatesMutex.Unlock()
	fake.SendUpdatesStub = nil
	if fake.sendUpdatesReturnsOnCall == nil {
		fake.sendUpdatesReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.sendUpdatesReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaJsonSender) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.sendDeleteMutex.RLock()
	defer fake.sendDeleteMutex.RUnlock()
	fake.sendDeletesMutex.RLock()
	defer fake.sendDeletesMutex.RUnlock()
	fake.sendUpdateMutex.RLock()
	defer fake.sendUpdateMutex.RUnlock()
	fake.sendUpdatesMutex.RLock()
	defer fake.sendUpdatesMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *KafkaJsonSender) recordInvocation(key string, args []interface{}) {
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

var _ kafka.JsonSender = new(KafkaJsonSender)
