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
func WithFlowFunction[IK any, IV any, OK any, OV any, T any](flowFunction task.FlowFunction[IK, IV, OK, OV, T]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.next = flowFunction
		return p
	}
}

func WithInputKeyFormat[IK any, IV any, OK any, OV any, T any](f format.Format[IK]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.ik = f
		return p
	}
}

func WithInputValueFormat[IK any, IV any, OK any, OV any, T any](f format.Format[IV]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.iv = f
		return p
	}
}

func WithOutputKeyFormat[IK any, IV any, OK any, OV any, T any](f format.Format[OK]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.ok = f
		return p
	}
}

func WithOutputValueFormat[IK any, IV any, OK any, OV any, T any](f format.Format[OV]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.ov = f
		return p
	}
}

func WithTaskValueFormat[IK any, IV any, OK any, OV any, T any](f format.Format[T]) runtime.Configuration[*TaskFlow[IK, IV, OK, OV, T]] {
	return func(p *TaskFlow[IK, IV, OK, OV, T]) *TaskFlow[IK, IV, OK, OV, T] {
		p.t = f
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
	next     task.FlowFunction[IK, IV, OK, OV, T]
	ik       format.Format[IK]
	iv       format.Format[IV]
	ok       format.Format[OK]
	ov       format.Format[OV]
	t        format.Format[T]
	producer task.Producer
}

func (r *TaskFlow[IK, IV, OK, OV, T]) Apply(c context.Context, ms []flow.Message[structure.Bytes, structure.Bytes]) ([]flow.Message[structure.Bytes, structure.Bytes], error) {
	byteResultMessages := []flow.Message[[]byte, []byte]{}
	allTasks := []task.Message[[]byte]{}

	for _, m := range ms {
		formattedMessage, unmarshalError := flow.Convert(m, format.Bytes(), format.Bytes(), r.ik, r.iv)
		if unmarshalError != nil {
			return flow.EmptySlice(), unmarshalError
		}

		nextTask, nextMessage, nextErr := r.next(c, formattedMessage)
		if nextErr != nil {
			return flow.EmptySlice(), nextErr
		}

		taskBytes, taskConversionError := task.Convert(nextTask, r.t, format.Bytes())
		if taskConversionError != nil {
			return flow.EmptySlice(), taskConversionError
		}
		allTasks = append(allTasks, taskBytes)

		bytesResMessage, marshalError := flow.Convert(nextMessage, r.ok, r.ov, format.Bytes(), format.Bytes())
		if marshalError != nil {
			return flow.EmptySlice(), marshalError
		}
		byteResultMessages = append(byteResultMessages, bytesResMessage)
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
