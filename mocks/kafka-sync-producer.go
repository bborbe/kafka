// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"

	"github.com/IBM/sarama"
	"github.com/bborbe/kafka"
)

type KafkaSyncProducer struct {
	CloseStub        func() error
	closeMutex       sync.RWMutex
	closeArgsForCall []struct {
	}
	closeReturns struct {
		result1 error
	}
	closeReturnsOnCall map[int]struct {
		result1 error
	}
	SendMessageStub        func(*sarama.ProducerMessage) (int32, int64, error)
	sendMessageMutex       sync.RWMutex
	sendMessageArgsForCall []struct {
		arg1 *sarama.ProducerMessage
	}
	sendMessageReturns struct {
		result1 int32
		result2 int64
		result3 error
	}
	sendMessageReturnsOnCall map[int]struct {
		result1 int32
		result2 int64
		result3 error
	}
	SendMessagesStub        func([]*sarama.ProducerMessage) error
	sendMessagesMutex       sync.RWMutex
	sendMessagesArgsForCall []struct {
		arg1 []*sarama.ProducerMessage
	}
	sendMessagesReturns struct {
		result1 error
	}
	sendMessagesReturnsOnCall map[int]struct {
		result1 error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *KafkaSyncProducer) Close() error {
	fake.closeMutex.Lock()
	ret, specificReturn := fake.closeReturnsOnCall[len(fake.closeArgsForCall)]
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	stub := fake.CloseStub
	fakeReturns := fake.closeReturns
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSyncProducer) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *KafkaSyncProducer) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *KafkaSyncProducer) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSyncProducer) CloseReturnsOnCall(i int, result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	if fake.closeReturnsOnCall == nil {
		fake.closeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.closeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSyncProducer) SendMessage(arg1 *sarama.ProducerMessage) (int32, int64, error) {
	fake.sendMessageMutex.Lock()
	ret, specificReturn := fake.sendMessageReturnsOnCall[len(fake.sendMessageArgsForCall)]
	fake.sendMessageArgsForCall = append(fake.sendMessageArgsForCall, struct {
		arg1 *sarama.ProducerMessage
	}{arg1})
	stub := fake.SendMessageStub
	fakeReturns := fake.sendMessageReturns
	fake.recordInvocation("SendMessage", []interface{}{arg1})
	fake.sendMessageMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2, ret.result3
	}
	return fakeReturns.result1, fakeReturns.result2, fakeReturns.result3
}

func (fake *KafkaSyncProducer) SendMessageCallCount() int {
	fake.sendMessageMutex.RLock()
	defer fake.sendMessageMutex.RUnlock()
	return len(fake.sendMessageArgsForCall)
}

func (fake *KafkaSyncProducer) SendMessageCalls(stub func(*sarama.ProducerMessage) (int32, int64, error)) {
	fake.sendMessageMutex.Lock()
	defer fake.sendMessageMutex.Unlock()
	fake.SendMessageStub = stub
}

func (fake *KafkaSyncProducer) SendMessageArgsForCall(i int) *sarama.ProducerMessage {
	fake.sendMessageMutex.RLock()
	defer fake.sendMessageMutex.RUnlock()
	argsForCall := fake.sendMessageArgsForCall[i]
	return argsForCall.arg1
}

func (fake *KafkaSyncProducer) SendMessageReturns(result1 int32, result2 int64, result3 error) {
	fake.sendMessageMutex.Lock()
	defer fake.sendMessageMutex.Unlock()
	fake.SendMessageStub = nil
	fake.sendMessageReturns = struct {
		result1 int32
		result2 int64
		result3 error
	}{result1, result2, result3}
}

func (fake *KafkaSyncProducer) SendMessageReturnsOnCall(i int, result1 int32, result2 int64, result3 error) {
	fake.sendMessageMutex.Lock()
	defer fake.sendMessageMutex.Unlock()
	fake.SendMessageStub = nil
	if fake.sendMessageReturnsOnCall == nil {
		fake.sendMessageReturnsOnCall = make(map[int]struct {
			result1 int32
			result2 int64
			result3 error
		})
	}
	fake.sendMessageReturnsOnCall[i] = struct {
		result1 int32
		result2 int64
		result3 error
	}{result1, result2, result3}
}

func (fake *KafkaSyncProducer) SendMessages(arg1 []*sarama.ProducerMessage) error {
	var arg1Copy []*sarama.ProducerMessage
	if arg1 != nil {
		arg1Copy = make([]*sarama.ProducerMessage, len(arg1))
		copy(arg1Copy, arg1)
	}
	fake.sendMessagesMutex.Lock()
	ret, specificReturn := fake.sendMessagesReturnsOnCall[len(fake.sendMessagesArgsForCall)]
	fake.sendMessagesArgsForCall = append(fake.sendMessagesArgsForCall, struct {
		arg1 []*sarama.ProducerMessage
	}{arg1Copy})
	stub := fake.SendMessagesStub
	fakeReturns := fake.sendMessagesReturns
	fake.recordInvocation("SendMessages", []interface{}{arg1Copy})
	fake.sendMessagesMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSyncProducer) SendMessagesCallCount() int {
	fake.sendMessagesMutex.RLock()
	defer fake.sendMessagesMutex.RUnlock()
	return len(fake.sendMessagesArgsForCall)
}

func (fake *KafkaSyncProducer) SendMessagesCalls(stub func([]*sarama.ProducerMessage) error) {
	fake.sendMessagesMutex.Lock()
	defer fake.sendMessagesMutex.Unlock()
	fake.SendMessagesStub = stub
}

func (fake *KafkaSyncProducer) SendMessagesArgsForCall(i int) []*sarama.ProducerMessage {
	fake.sendMessagesMutex.RLock()
	defer fake.sendMessagesMutex.RUnlock()
	argsForCall := fake.sendMessagesArgsForCall[i]
	return argsForCall.arg1
}

func (fake *KafkaSyncProducer) SendMessagesReturns(result1 error) {
	fake.sendMessagesMutex.Lock()
	defer fake.sendMessagesMutex.Unlock()
	fake.SendMessagesStub = nil
	fake.sendMessagesReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSyncProducer) SendMessagesReturnsOnCall(i int, result1 error) {
	fake.sendMessagesMutex.Lock()
	defer fake.sendMessagesMutex.Unlock()
	fake.SendMessagesStub = nil
	if fake.sendMessagesReturnsOnCall == nil {
		fake.sendMessagesReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.sendMessagesReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSyncProducer) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.sendMessageMutex.RLock()
	defer fake.sendMessageMutex.RUnlock()
	fake.sendMessagesMutex.RLock()
	defer fake.sendMessagesMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *KafkaSyncProducer) recordInvocation(key string, args []interface{}) {
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

var _ kafka.SyncProducer = new(KafkaSyncProducer)