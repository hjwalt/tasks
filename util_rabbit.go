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
	QualifierRabbitProducer         = "QualifierRabbitProducer"
	QualifierRabbitConsumerExecutor = "QualifierRabbitConsumerExecutor"
	QualifierRabbitConsumer         = "QualifierRabbitConsumer"
)

// Producer

func RegisterProducer(
	container inverse.Container,
	name string,
	rabbitConnectionString string,
	configs []runtime.Configuration[*runtime_rabbit.Producer],
) {

	resolver := runtime.NewResolver[*runtime_rabbit.Producer, task.Producer](
		QualifierRabbitProducer,
		container,
		true,
		runtime_rabbit.NewProducer,
	)

	resolver.AddConfigVal(runtime_rabbit.WithProducerName(name))
	resolver.AddConfigVal(runtime_rabbit.WithProducerConnectionString(rabbitConnectionString))

	for _, config := range configs {
		resolver.AddConfigVal(config)
	}

	resolver.Register()

	flows.RegisterRuntime(QualifierRabbitProducer, container)
}

func GetRabbitProducer(ctx context.Context, ci inverse.Container) (task.Producer, error) {
	return inverse.GenericGetLast[task.Producer](ci, ctx, QualifierRabbitProducer)
}

// Consumer

func RegisterConsumer(
	container inverse.Container,
	name string,
	rabbitConnectionString string,
	channel string,
	configs []runtime.Configuration[*runtime_rabbit.Consumer],
) {

	resolver := runtime.NewResolver[*runtime_rabbit.Consumer, runtime.Runtime](
		QualifierRabbitConsumer,
		container,
		true,
		runtime_rabbit.NewConsumer,
	)

	resolver.AddConfigVal(runtime_rabbit.WithConsumerName(name))
	resolver.AddConfigVal(runtime_rabbit.WithConsumerQueueName(channel))
	resolver.AddConfigVal(runtime_rabbit.WithConsumerConnectionString(rabbitConnectionString))
	resolver.AddConfig(InjectorConsumerExecutorConfiguration)

	for _, config := range configs {
		resolver.AddConfigVal(config)
	}

	resolver.Register()

	flows.RegisterRuntime(QualifierRabbitConsumer, container)
}

func RegisterConsumerExecutor(ci inverse.Container, injector func(ctx context.Context, ci inverse.Container) (task.Executor[structure.Bytes], error)) {
	inverse.GenericAdd(ci, QualifierRabbitConsumerExecutor, injector)
}

func InjectorConsumerExecutorConfiguration(ctx context.Context, ci inverse.Container) (runtime.Configuration[*runtime_rabbit.Consumer], error) {
	handler, getHandlerError := inverse.GenericGetLast[task.Executor[structure.Bytes]](ci, ctx, QualifierRabbitConsumerExecutor)
	if getHandlerError != nil {
		return nil, getHandlerError
	}
	return runtime_rabbit.WithConsumerHandler(handler), nil
}

// ===================================

func RegisterProducerConfig(ci inverse.Container, configs ...runtime.Configuration[*runtime_rabbit.Producer]) {
	for _, config := range configs {
		ci.AddVal(runtime.QualifierConfig(QualifierRabbitProducer), config)
	}
}

func RegisterConsumerConfig(ci inverse.Container, configs ...runtime.Configuration[*runtime_rabbit.Consumer]) {
	for _, config := range configs {
		ci.AddVal(runtime.QualifierConfig(QualifierRabbitConsumer), config)
	}
}
