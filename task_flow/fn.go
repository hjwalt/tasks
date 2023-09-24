package task_flow

import (
	"context"

	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/stateless"
	"github.com/hjwalt/runway/format"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/task"
)

type FlowFunction[IK any, IV any, OK any, OV any, T any] func(context.Context, flow.Message[IK, IV]) (*task.Message[T], *flow.Message[OK, OV], error)

// constructor
func NewTaskFlow[IK any, IV any, OK any, OV any, T any](c ...runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]]) stateless.BatchFunction {
	return runtime.ConstructorFor[*TaskFlow[IK, IV, OK, OV, T], stateless.BatchFunction](
		func() *TaskFlow[IK, IV, OK, OV, T] {
			return &TaskFlow[IK, IV, OK, OV, T]{}
		},
		func(hr *TaskFlow[IK, IV, OK, OV, T]) stateless.BatchFunction {
			return hr.Apply
		},
	)(c...)
}

// configuration
func WithFlowFunction[IK any, IV any, OK any, OV any, T any](flowFunction FlowFunction[IK, IV, OK, OV, T]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.next = flowFunction
		return p
	}
}

func WithInputTopic[IK any, IV any, OK any, OV any, T any](t flow.Topic[IK, IV]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.inputTopic = t
		return p
	}
}

func WithOutputTopic[IK any, IV any, OK any, OV any, T any](t flow.Topic[OK, OV]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.outputTopic = t
		return p
	}
}

func WithTaskChannel[IK any, IV any, OK any, OV any, T any](t task.Channel[T]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.taskChannel = t
		return p
	}
}

func WithProducer[IK any, IV any, OK any, OV any, T any](producer task.Producer) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.producer = producer
		return p
	}
}

// implementation
type TaskFlow[IK any, IV any, OK any, OV any, T any] struct {
	next        FlowFunction[IK, IV, OK, OV, T]
	inputTopic  flow.Topic[IK, IV]
	outputTopic flow.Topic[OK, OV]
	taskChannel task.Channel[T]
	producer    task.Producer
}

func (r *TaskFlow[IK, IV, OK, OV, T]) Apply(c context.Context, ms []flow.Message[structure.Bytes, structure.Bytes]) ([]flow.Message[structure.Bytes, structure.Bytes], error) {
	byteResultMessages := []flow.Message[[]byte, []byte]{}
	allTasks := []task.Message[[]byte]{}

	for _, m := range ms {
		formattedMessage, unmarshalError := flow.Convert(m, format.Bytes(), format.Bytes(), r.inputTopic.KeyFormat(), r.inputTopic.ValueFormat())
		if unmarshalError != nil {
			return flow.EmptySlice(), unmarshalError
		}

		nextTask, nextMessage, nextErr := r.next(c, formattedMessage)
		if nextErr != nil {
			return flow.EmptySlice(), nextErr
		}

		if nextTask != nil {
			taskBytes, taskConversionError := task.Convert(*nextTask, r.taskChannel.ValueFormat(), format.Bytes())
			if taskConversionError != nil {
				return flow.EmptySlice(), taskConversionError
			}
			taskBytes.Channel = r.taskChannel.Name()
			allTasks = append(allTasks, taskBytes)
		}

		if nextMessage != nil {
			bytesResMessage, marshalError := flow.Convert(*nextMessage, r.outputTopic.KeyFormat(), r.outputTopic.ValueFormat(), format.Bytes(), format.Bytes())
			if marshalError != nil {
				return flow.EmptySlice(), marshalError
			}
			bytesResMessage.Topic = r.outputTopic.Name()
			byteResultMessages = append(byteResultMessages, bytesResMessage)
		}
	}

	// isolating produce errors
	for _, taskBytes := range allTasks {
		produceErr := r.producer.Produce(c, taskBytes)
		if produceErr != nil {
			return flow.EmptySlice(), produceErr
		}
	}

	return byteResultMessages, nil
}
