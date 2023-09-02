package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/stateless"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
	"github.com/hjwalt/tasks/task_flow"
)

const (
	QualifierRabbitProducerConfiguration = "QualifierRabbitProducerConfiguration"
	QualifierRabbitProducer              = "QualifierRabbitProducer"
	QualifierRabbitConsumerConfiguration = "QualifierRabbitConsumerConfiguration"
	QualifierRabbitConsumerExecutor      = "QualifierRabbitConsumerExecutor"
	QualifierRabbitConsumer              = "QualifierRabbitConsumer"
)

// Producer
func RegisterProducerConfig(config ...runtime.Configuration[*runtime_rabbit.Producer]) {
	inverse.RegisterInstances(QualifierRabbitProducerConfiguration, config)
}

func RegisterProducer() {
	inverse.RegisterWithConfigurationRequired(
		QualifierRabbitProducer,
		QualifierRabbitProducerConfiguration,
		runtime_rabbit.NewProducer,
	)
	inverse.Register(flows.QualifierRuntime, flows.InjectorRuntime(QualifierRabbitProducer))
}

func GetRabbitProducer(ctx context.Context) (task.Producer, error) {
	return inverse.GetLast[task.Producer](ctx, QualifierRabbitProducer)
}

// Consumer
func RegisterConsumerConfig(config ...runtime.Configuration[*runtime_rabbit.Consumer]) {
	inverse.RegisterInstances(QualifierRabbitConsumerConfiguration, config)
}

func RegisterConsumer() {
	inverse.RegisterWithConfigurationRequired(
		QualifierRabbitConsumer,
		QualifierRabbitConsumerConfiguration,
		runtime_rabbit.NewConsumer,
	)
	inverse.Register(flows.QualifierRuntime, flows.InjectorRuntime(QualifierRabbitConsumer))
	inverse.Register[runtime.Configuration[*runtime_rabbit.Consumer]](QualifierRabbitConsumerConfiguration, InjectorConsumerExecutorConfiguration)
}

func RegisterConsumerExecutor(injector func(ctx context.Context) (task.Executor[structure.Bytes], error)) {
	inverse.Register(QualifierRabbitConsumerExecutor, injector)
}

func InjectorConsumerExecutorConfiguration(ctx context.Context) (runtime.Configuration[*runtime_rabbit.Consumer], error) {
	handler, getHandlerError := inverse.GetLast[task.Executor[structure.Bytes]](ctx, QualifierRabbitConsumerExecutor)
	if getHandlerError != nil {
		return nil, getHandlerError
	}
	return runtime_rabbit.WithConsumerHandler(handler), nil
}

func RegisterTaskFlowFunction[IK any, IV any, OK any, OV any, T any](
	fn task_flow.FlowFunction[IK, IV, OK, OV, T],
	inputTopic flow.Topic[IK, IV],
	outputTopic flow.Topic[OK, OV],
	taskChannel task.Channel[T],
) {
	flows.RegisterConsumerFunctionInjector(
		func(ctx context.Context) (flows.ConsumerFunction, error) {
			taskProducer, err := GetRabbitProducer(ctx)
			if err != nil {
				return flows.ConsumerFunction{}, err
			}

			wrappedBatch := task_flow.NewTaskFlow[IK, IV, OK, OV, T](
				task_flow.WithFlowFunction[IK, IV, OK, OV, T](fn),
				task_flow.WithProducer[IK, IV, OK, OV, T](taskProducer),
				task_flow.WithInputTopic[IK, IV, OK, OV, T](inputTopic),
				task_flow.WithOutputTopic[IK, IV, OK, OV, T](outputTopic),
				task_flow.WithTaskChannel[IK, IV, OK, OV, T](taskChannel),
			)

			return flows.ConsumerFunction{
				Topic: inputTopic.Name(),
				Key:   stateless.Base64PersistenceId,
				Fn:    wrappedBatch,
			}, nil
		},
	)
}
