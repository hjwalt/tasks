package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_cron"
)

const (
	QualifierCronConfiguration = "QualifierCronConfiguration"
	QualifierCron              = "QualifierCron"
)

func RegisterCronConfigDefault() {
	inverse.Register(QualifierCronConfiguration, InjectorCronFlowProducer)
	inverse.Register(QualifierCronConfiguration, InjectorCronTaskProducer)
}

func RegisterCronConfig(config runtime.Configuration[*runtime_cron.Cron]) {
	inverse.RegisterInstance(QualifierCronConfiguration, config)
}

func RegisterCron() {
	inverse.RegisterWithConfigurationRequired(
		QualifierCron,
		QualifierCronConfiguration,
		runtime_cron.NewCron,
	)
	inverse.Register(flows.QualifierRuntime, flows.InjectorRuntime(QualifierCron))
}

func InjectorCronFlowProducer(ctx context.Context) (runtime.Configuration[*runtime_cron.Cron], error) {
	handler, getHandlerError := flows.GetKafkaProducer(ctx)
	if getHandlerError != nil {
		return nil, getHandlerError
	}
	return runtime_cron.WithFlowProducer(handler), nil
}

func InjectorCronTaskProducer(ctx context.Context) (runtime.Configuration[*runtime_cron.Cron], error) {
	handler, getHandlerError := GetRabbitProducer(ctx)
	if getHandlerError != nil {
		return nil, getHandlerError
	}
	return runtime_cron.WithTaskProducer(handler), nil
}
