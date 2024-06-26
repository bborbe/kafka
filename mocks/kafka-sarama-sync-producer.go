// Code generated by counterfeiter. DO NOT EDIT.
package mocks

import (
	"sync"

	"github.com/IBM/sarama"
	"github.com/bborbe/kafka"
)

type KafkaSaramaSyncProducer struct {
	AbortTxnStub        func() error
	abortTxnMutex       sync.RWMutex
	abortTxnArgsForCall []struct {
	}
	abortTxnReturns struct {
		result1 error
	}
	abortTxnReturnsOnCall map[int]struct {
		result1 error
	}
	AddMessageToTxnStub        func(*sarama.ConsumerMessage, string, *string) error
	addMessageToTxnMutex       sync.RWMutex
	addMessageToTxnArgsForCall []struct {
		arg1 *sarama.ConsumerMessage
		arg2 string
		arg3 *string
	}
	addMessageToTxnReturns struct {
		result1 error
	}
	addMessageToTxnReturnsOnCall map[int]struct {
		result1 error
	}
	AddOffsetsToTxnStub        func(map[string][]*sarama.PartitionOffsetMetadata, string) error
	addOffsetsToTxnMutex       sync.RWMutex
	addOffsetsToTxnArgsForCall []struct {
		arg1 map[string][]*sarama.PartitionOffsetMetadata
		arg2 string
	}
	addOffsetsToTxnReturns struct {
		result1 error
	}
	addOffsetsToTxnReturnsOnCall map[int]struct {
		result1 error
	}
	BeginTxnStub        func() error
	beginTxnMutex       sync.RWMutex
	beginTxnArgsForCall []struct {
	}
	beginTxnReturns struct {
		result1 error
	}
	beginTxnReturnsOnCall map[int]struct {
		result1 error
	}
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
	CommitTxnStub        func() error
	commitTxnMutex       sync.RWMutex
	commitTxnArgsForCall []struct {
	}
	commitTxnReturns struct {
		result1 error
	}
	commitTxnReturnsOnCall map[int]struct {
		result1 error
	}
	IsTransactionalStub        func() bool
	isTransactionalMutex       sync.RWMutex
	isTransactionalArgsForCall []struct {
	}
	isTransactionalReturns struct {
		result1 bool
	}
	isTransactionalReturnsOnCall map[int]struct {
		result1 bool
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
	TxnStatusStub        func() sarama.ProducerTxnStatusFlag
	txnStatusMutex       sync.RWMutex
	txnStatusArgsForCall []struct {
	}
	txnStatusReturns struct {
		result1 sarama.ProducerTxnStatusFlag
	}
	txnStatusReturnsOnCall map[int]struct {
		result1 sarama.ProducerTxnStatusFlag
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *KafkaSaramaSyncProducer) AbortTxn() error {
	fake.abortTxnMutex.Lock()
	ret, specificReturn := fake.abortTxnReturnsOnCall[len(fake.abortTxnArgsForCall)]
	fake.abortTxnArgsForCall = append(fake.abortTxnArgsForCall, struct {
	}{})
	stub := fake.AbortTxnStub
	fakeReturns := fake.abortTxnReturns
	fake.recordInvocation("AbortTxn", []interface{}{})
	fake.abortTxnMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) AbortTxnCallCount() int {
	fake.abortTxnMutex.RLock()
	defer fake.abortTxnMutex.RUnlock()
	return len(fake.abortTxnArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) AbortTxnCalls(stub func() error) {
	fake.abortTxnMutex.Lock()
	defer fake.abortTxnMutex.Unlock()
	fake.AbortTxnStub = stub
}

func (fake *KafkaSaramaSyncProducer) AbortTxnReturns(result1 error) {
	fake.abortTxnMutex.Lock()
	defer fake.abortTxnMutex.Unlock()
	fake.AbortTxnStub = nil
	fake.abortTxnReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) AbortTxnReturnsOnCall(i int, result1 error) {
	fake.abortTxnMutex.Lock()
	defer fake.abortTxnMutex.Unlock()
	fake.AbortTxnStub = nil
	if fake.abortTxnReturnsOnCall == nil {
		fake.abortTxnReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.abortTxnReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) AddMessageToTxn(arg1 *sarama.ConsumerMessage, arg2 string, arg3 *string) error {
	fake.addMessageToTxnMutex.Lock()
	ret, specificReturn := fake.addMessageToTxnReturnsOnCall[len(fake.addMessageToTxnArgsForCall)]
	fake.addMessageToTxnArgsForCall = append(fake.addMessageToTxnArgsForCall, struct {
		arg1 *sarama.ConsumerMessage
		arg2 string
		arg3 *string
	}{arg1, arg2, arg3})
	stub := fake.AddMessageToTxnStub
	fakeReturns := fake.addMessageToTxnReturns
	fake.recordInvocation("AddMessageToTxn", []interface{}{arg1, arg2, arg3})
	fake.addMessageToTxnMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) AddMessageToTxnCallCount() int {
	fake.addMessageToTxnMutex.RLock()
	defer fake.addMessageToTxnMutex.RUnlock()
	return len(fake.addMessageToTxnArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) AddMessageToTxnCalls(stub func(*sarama.ConsumerMessage, string, *string) error) {
	fake.addMessageToTxnMutex.Lock()
	defer fake.addMessageToTxnMutex.Unlock()
	fake.AddMessageToTxnStub = stub
}

func (fake *KafkaSaramaSyncProducer) AddMessageToTxnArgsForCall(i int) (*sarama.ConsumerMessage, string, *string) {
	fake.addMessageToTxnMutex.RLock()
	defer fake.addMessageToTxnMutex.RUnlock()
	argsForCall := fake.addMessageToTxnArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *KafkaSaramaSyncProducer) AddMessageToTxnReturns(result1 error) {
	fake.addMessageToTxnMutex.Lock()
	defer fake.addMessageToTxnMutex.Unlock()
	fake.AddMessageToTxnStub = nil
	fake.addMessageToTxnReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) AddMessageToTxnReturnsOnCall(i int, result1 error) {
	fake.addMessageToTxnMutex.Lock()
	defer fake.addMessageToTxnMutex.Unlock()
	fake.AddMessageToTxnStub = nil
	if fake.addMessageToTxnReturnsOnCall == nil {
		fake.addMessageToTxnReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.addMessageToTxnReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) AddOffsetsToTxn(arg1 map[string][]*sarama.PartitionOffsetMetadata, arg2 string) error {
	fake.addOffsetsToTxnMutex.Lock()
	ret, specificReturn := fake.addOffsetsToTxnReturnsOnCall[len(fake.addOffsetsToTxnArgsForCall)]
	fake.addOffsetsToTxnArgsForCall = append(fake.addOffsetsToTxnArgsForCall, struct {
		arg1 map[string][]*sarama.PartitionOffsetMetadata
		arg2 string
	}{arg1, arg2})
	stub := fake.AddOffsetsToTxnStub
	fakeReturns := fake.addOffsetsToTxnReturns
	fake.recordInvocation("AddOffsetsToTxn", []interface{}{arg1, arg2})
	fake.addOffsetsToTxnMutex.Unlock()
	if stub != nil {
		return stub(arg1, arg2)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) AddOffsetsToTxnCallCount() int {
	fake.addOffsetsToTxnMutex.RLock()
	defer fake.addOffsetsToTxnMutex.RUnlock()
	return len(fake.addOffsetsToTxnArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) AddOffsetsToTxnCalls(stub func(map[string][]*sarama.PartitionOffsetMetadata, string) error) {
	fake.addOffsetsToTxnMutex.Lock()
	defer fake.addOffsetsToTxnMutex.Unlock()
	fake.AddOffsetsToTxnStub = stub
}

func (fake *KafkaSaramaSyncProducer) AddOffsetsToTxnArgsForCall(i int) (map[string][]*sarama.PartitionOffsetMetadata, string) {
	fake.addOffsetsToTxnMutex.RLock()
	defer fake.addOffsetsToTxnMutex.RUnlock()
	argsForCall := fake.addOffsetsToTxnArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2
}

func (fake *KafkaSaramaSyncProducer) AddOffsetsToTxnReturns(result1 error) {
	fake.addOffsetsToTxnMutex.Lock()
	defer fake.addOffsetsToTxnMutex.Unlock()
	fake.AddOffsetsToTxnStub = nil
	fake.addOffsetsToTxnReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) AddOffsetsToTxnReturnsOnCall(i int, result1 error) {
	fake.addOffsetsToTxnMutex.Lock()
	defer fake.addOffsetsToTxnMutex.Unlock()
	fake.AddOffsetsToTxnStub = nil
	if fake.addOffsetsToTxnReturnsOnCall == nil {
		fake.addOffsetsToTxnReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.addOffsetsToTxnReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) BeginTxn() error {
	fake.beginTxnMutex.Lock()
	ret, specificReturn := fake.beginTxnReturnsOnCall[len(fake.beginTxnArgsForCall)]
	fake.beginTxnArgsForCall = append(fake.beginTxnArgsForCall, struct {
	}{})
	stub := fake.BeginTxnStub
	fakeReturns := fake.beginTxnReturns
	fake.recordInvocation("BeginTxn", []interface{}{})
	fake.beginTxnMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) BeginTxnCallCount() int {
	fake.beginTxnMutex.RLock()
	defer fake.beginTxnMutex.RUnlock()
	return len(fake.beginTxnArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) BeginTxnCalls(stub func() error) {
	fake.beginTxnMutex.Lock()
	defer fake.beginTxnMutex.Unlock()
	fake.BeginTxnStub = stub
}

func (fake *KafkaSaramaSyncProducer) BeginTxnReturns(result1 error) {
	fake.beginTxnMutex.Lock()
	defer fake.beginTxnMutex.Unlock()
	fake.BeginTxnStub = nil
	fake.beginTxnReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) BeginTxnReturnsOnCall(i int, result1 error) {
	fake.beginTxnMutex.Lock()
	defer fake.beginTxnMutex.Unlock()
	fake.BeginTxnStub = nil
	if fake.beginTxnReturnsOnCall == nil {
		fake.beginTxnReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.beginTxnReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) Close() error {
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

func (fake *KafkaSaramaSyncProducer) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *KafkaSaramaSyncProducer) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) CloseReturnsOnCall(i int, result1 error) {
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

func (fake *KafkaSaramaSyncProducer) CommitTxn() error {
	fake.commitTxnMutex.Lock()
	ret, specificReturn := fake.commitTxnReturnsOnCall[len(fake.commitTxnArgsForCall)]
	fake.commitTxnArgsForCall = append(fake.commitTxnArgsForCall, struct {
	}{})
	stub := fake.CommitTxnStub
	fakeReturns := fake.commitTxnReturns
	fake.recordInvocation("CommitTxn", []interface{}{})
	fake.commitTxnMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) CommitTxnCallCount() int {
	fake.commitTxnMutex.RLock()
	defer fake.commitTxnMutex.RUnlock()
	return len(fake.commitTxnArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) CommitTxnCalls(stub func() error) {
	fake.commitTxnMutex.Lock()
	defer fake.commitTxnMutex.Unlock()
	fake.CommitTxnStub = stub
}

func (fake *KafkaSaramaSyncProducer) CommitTxnReturns(result1 error) {
	fake.commitTxnMutex.Lock()
	defer fake.commitTxnMutex.Unlock()
	fake.CommitTxnStub = nil
	fake.commitTxnReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) CommitTxnReturnsOnCall(i int, result1 error) {
	fake.commitTxnMutex.Lock()
	defer fake.commitTxnMutex.Unlock()
	fake.CommitTxnStub = nil
	if fake.commitTxnReturnsOnCall == nil {
		fake.commitTxnReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.commitTxnReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) IsTransactional() bool {
	fake.isTransactionalMutex.Lock()
	ret, specificReturn := fake.isTransactionalReturnsOnCall[len(fake.isTransactionalArgsForCall)]
	fake.isTransactionalArgsForCall = append(fake.isTransactionalArgsForCall, struct {
	}{})
	stub := fake.IsTransactionalStub
	fakeReturns := fake.isTransactionalReturns
	fake.recordInvocation("IsTransactional", []interface{}{})
	fake.isTransactionalMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) IsTransactionalCallCount() int {
	fake.isTransactionalMutex.RLock()
	defer fake.isTransactionalMutex.RUnlock()
	return len(fake.isTransactionalArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) IsTransactionalCalls(stub func() bool) {
	fake.isTransactionalMutex.Lock()
	defer fake.isTransactionalMutex.Unlock()
	fake.IsTransactionalStub = stub
}

func (fake *KafkaSaramaSyncProducer) IsTransactionalReturns(result1 bool) {
	fake.isTransactionalMutex.Lock()
	defer fake.isTransactionalMutex.Unlock()
	fake.IsTransactionalStub = nil
	fake.isTransactionalReturns = struct {
		result1 bool
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) IsTransactionalReturnsOnCall(i int, result1 bool) {
	fake.isTransactionalMutex.Lock()
	defer fake.isTransactionalMutex.Unlock()
	fake.IsTransactionalStub = nil
	if fake.isTransactionalReturnsOnCall == nil {
		fake.isTransactionalReturnsOnCall = make(map[int]struct {
			result1 bool
		})
	}
	fake.isTransactionalReturnsOnCall[i] = struct {
		result1 bool
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) SendMessage(arg1 *sarama.ProducerMessage) (int32, int64, error) {
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

func (fake *KafkaSaramaSyncProducer) SendMessageCallCount() int {
	fake.sendMessageMutex.RLock()
	defer fake.sendMessageMutex.RUnlock()
	return len(fake.sendMessageArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) SendMessageCalls(stub func(*sarama.ProducerMessage) (int32, int64, error)) {
	fake.sendMessageMutex.Lock()
	defer fake.sendMessageMutex.Unlock()
	fake.SendMessageStub = stub
}

func (fake *KafkaSaramaSyncProducer) SendMessageArgsForCall(i int) *sarama.ProducerMessage {
	fake.sendMessageMutex.RLock()
	defer fake.sendMessageMutex.RUnlock()
	argsForCall := fake.sendMessageArgsForCall[i]
	return argsForCall.arg1
}

func (fake *KafkaSaramaSyncProducer) SendMessageReturns(result1 int32, result2 int64, result3 error) {
	fake.sendMessageMutex.Lock()
	defer fake.sendMessageMutex.Unlock()
	fake.SendMessageStub = nil
	fake.sendMessageReturns = struct {
		result1 int32
		result2 int64
		result3 error
	}{result1, result2, result3}
}

func (fake *KafkaSaramaSyncProducer) SendMessageReturnsOnCall(i int, result1 int32, result2 int64, result3 error) {
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

func (fake *KafkaSaramaSyncProducer) SendMessages(arg1 []*sarama.ProducerMessage) error {
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

func (fake *KafkaSaramaSyncProducer) SendMessagesCallCount() int {
	fake.sendMessagesMutex.RLock()
	defer fake.sendMessagesMutex.RUnlock()
	return len(fake.sendMessagesArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) SendMessagesCalls(stub func([]*sarama.ProducerMessage) error) {
	fake.sendMessagesMutex.Lock()
	defer fake.sendMessagesMutex.Unlock()
	fake.SendMessagesStub = stub
}

func (fake *KafkaSaramaSyncProducer) SendMessagesArgsForCall(i int) []*sarama.ProducerMessage {
	fake.sendMessagesMutex.RLock()
	defer fake.sendMessagesMutex.RUnlock()
	argsForCall := fake.sendMessagesArgsForCall[i]
	return argsForCall.arg1
}

func (fake *KafkaSaramaSyncProducer) SendMessagesReturns(result1 error) {
	fake.sendMessagesMutex.Lock()
	defer fake.sendMessagesMutex.Unlock()
	fake.SendMessagesStub = nil
	fake.sendMessagesReturns = struct {
		result1 error
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) SendMessagesReturnsOnCall(i int, result1 error) {
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

func (fake *KafkaSaramaSyncProducer) TxnStatus() sarama.ProducerTxnStatusFlag {
	fake.txnStatusMutex.Lock()
	ret, specificReturn := fake.txnStatusReturnsOnCall[len(fake.txnStatusArgsForCall)]
	fake.txnStatusArgsForCall = append(fake.txnStatusArgsForCall, struct {
	}{})
	stub := fake.TxnStatusStub
	fakeReturns := fake.txnStatusReturns
	fake.recordInvocation("TxnStatus", []interface{}{})
	fake.txnStatusMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *KafkaSaramaSyncProducer) TxnStatusCallCount() int {
	fake.txnStatusMutex.RLock()
	defer fake.txnStatusMutex.RUnlock()
	return len(fake.txnStatusArgsForCall)
}

func (fake *KafkaSaramaSyncProducer) TxnStatusCalls(stub func() sarama.ProducerTxnStatusFlag) {
	fake.txnStatusMutex.Lock()
	defer fake.txnStatusMutex.Unlock()
	fake.TxnStatusStub = stub
}

func (fake *KafkaSaramaSyncProducer) TxnStatusReturns(result1 sarama.ProducerTxnStatusFlag) {
	fake.txnStatusMutex.Lock()
	defer fake.txnStatusMutex.Unlock()
	fake.TxnStatusStub = nil
	fake.txnStatusReturns = struct {
		result1 sarama.ProducerTxnStatusFlag
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) TxnStatusReturnsOnCall(i int, result1 sarama.ProducerTxnStatusFlag) {
	fake.txnStatusMutex.Lock()
	defer fake.txnStatusMutex.Unlock()
	fake.TxnStatusStub = nil
	if fake.txnStatusReturnsOnCall == nil {
		fake.txnStatusReturnsOnCall = make(map[int]struct {
			result1 sarama.ProducerTxnStatusFlag
		})
	}
	fake.txnStatusReturnsOnCall[i] = struct {
		result1 sarama.ProducerTxnStatusFlag
	}{result1}
}

func (fake *KafkaSaramaSyncProducer) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.abortTxnMutex.RLock()
	defer fake.abortTxnMutex.RUnlock()
	fake.addMessageToTxnMutex.RLock()
	defer fake.addMessageToTxnMutex.RUnlock()
	fake.addOffsetsToTxnMutex.RLock()
	defer fake.addOffsetsToTxnMutex.RUnlock()
	fake.beginTxnMutex.RLock()
	defer fake.beginTxnMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.commitTxnMutex.RLock()
	defer fake.commitTxnMutex.RUnlock()
	fake.isTransactionalMutex.RLock()
	defer fake.isTransactionalMutex.RUnlock()
	fake.sendMessageMutex.RLock()
	defer fake.sendMessageMutex.RUnlock()
	fake.sendMessagesMutex.RLock()
	defer fake.sendMessagesMutex.RUnlock()
	fake.txnStatusMutex.RLock()
	defer fake.txnStatusMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *KafkaSaramaSyncProducer) recordInvocation(key string, args []interface{}) {
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

var _ kafka.SaramaSyncProducer = new(KafkaSaramaSyncProducer)
