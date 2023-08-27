package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
)

const (
	QualifierRabbitProducerConfiguration = "QualifierRabbitProducerConfiguration"
	QualifierRabbitProducer              = "QualifierRabbitProducer"
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
