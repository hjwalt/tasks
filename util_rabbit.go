package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
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
