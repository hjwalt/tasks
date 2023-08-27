package task_executor_bun_flow

import (
	"context"

	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/runtime_bun"
	"github.com/hjwalt/runway/format"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/task"
)

type Executor[OK any, OV any, T any] func(context.Context, task.Message[T], runtime_bun.BunConnection, flow.Producer) (flow.Message[OK, OV], error)

// constructor
func NewTaskExecutor[OK any, OV any, T any](c ...runtime.Configuration[*TaskExecutor[OK, OV, T]]) task.Executor[structure.Bytes] {
	return runtime.ConstructorFor[*TaskExecutor[OK, OV, T], task.Executor[structure.Bytes]](
		func() *TaskExecutor[OK, OV, T] {
			return &TaskExecutor[OK, OV, T]{}
		},
		func(hr *TaskExecutor[OK, OV, T]) task.Executor[structure.Bytes] {
			return hr.Apply
		},
	)(c...)
}

// configuration
func WithOutputTopic[OK any, OV any, T any](t flow.Topic[OK, OV]) runtime.Configuration[*TaskExecutor[OK, OV, T]] {
	return func(p *TaskExecutor[OK, OV, T]) *TaskExecutor[OK, OV, T] {
		p.outputTopic = t
		return p
	}
}

func WithTaskChannel[OK any, OV any, T any](t task.Channel[T]) runtime.Configuration[*TaskExecutor[OK, OV, T]] {
	return func(p *TaskExecutor[OK, OV, T]) *TaskExecutor[OK, OV, T] {
		p.taskChannel = t
		return p
	}
}

func WithProducer[OK any, OV any, T any](producer flow.Producer) runtime.Configuration[*TaskExecutor[OK, OV, T]] {
	return func(p *TaskExecutor[OK, OV, T]) *TaskExecutor[OK, OV, T] {
		p.producer = producer
		return p
	}
}

func WithBunConnection[OK any, OV any, T any](bun runtime_bun.BunConnection) runtime.Configuration[*TaskExecutor[OK, OV, T]] {
	return func(p *TaskExecutor[OK, OV, T]) *TaskExecutor[OK, OV, T] {
		p.bun = bun
		return p
	}
}

func WithExecutor[OK any, OV any, T any](executor Executor[OK, OV, T]) runtime.Configuration[*TaskExecutor[OK, OV, T]] {
	return func(p *TaskExecutor[OK, OV, T]) *TaskExecutor[OK, OV, T] {
		p.executor = executor
		return p
	}
}

// implementation
type TaskExecutor[OK any, OV any, T any] struct {
	outputTopic flow.Topic[OK, OV]
	taskChannel task.Channel[T]
	executor    Executor[OK, OV, T]
	bun         runtime_bun.BunConnection
	producer    flow.Producer
}

func (r *TaskExecutor[OK, OV, T]) Apply(c context.Context, t task.Message[structure.Bytes]) error {
	taskConverted, taskConversionError := task.Convert(t, format.Bytes(), r.taskChannel.ValueFormat())
	if taskConversionError != nil {
		return taskConversionError
	}

	outMessage, err := r.executor(c, taskConverted, r.bun, r.producer)
	if err != nil {
		return err
	}

	bytesResMessage, marshalError := flow.Convert(outMessage, r.outputTopic.KeyFormat(), r.outputTopic.ValueFormat(), format.Bytes(), format.Bytes())
	if marshalError != nil {
		return marshalError
	}
	bytesResMessage.Topic = r.outputTopic.Name()

	if produceErr := r.producer.Produce(c, []flow.Message[structure.Bytes, structure.Bytes]{bytesResMessage}); produceErr != nil {
		return produceErr
	}

	return nil
}
