package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_cron"
)

const (
	QualifierCron = "QualifierCron"
)

func RegisterCron(
	container inverse.Container,
	configs []runtime.Configuration[*runtime_cron.Cron],
) {

	resolver := runtime.NewResolver[*runtime_cron.Cron, runtime.Runtime](
		QualifierCron,
		container,
		true,
		runtime_cron.NewCron,
	)

	resolver.AddConfig(ResolveCronConfigTaskProducer)

	for _, config := range configs {
		resolver.AddConfigVal(config)
	}

	resolver.Register()

	flows.RegisterRuntime(QualifierCron, container)
}

func ResolveCronConfigTaskProducer(ctx context.Context, ci inverse.Container) (runtime.Configuration[*runtime_cron.Cron], error) {
	handler, getHandlerError := GetRabbitProducer(ctx, ci)
	if getHandlerError != nil {
		return nil, getHandlerError
	}
	return runtime_cron.WithTaskProducer(handler), nil
}

// ===================================

func RegisterCronConfig(ci inverse.Container, configs ...runtime.Configuration[*runtime_cron.Cron]) {
	for _, config := range configs {
		ci.AddVal(runtime.QualifierConfig(QualifierCron), config)
	}
}
