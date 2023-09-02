package tasks

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_cron"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
)

type CronConfiguration[OK any, OV any, T any] struct {
	Name                        string
	OutputTopic                 flow.Topic[OK, OV]
	TaskChannel                 task.Channel[T]
	Scheduler                   task.Scheduler[OK, OV, T]
	Schedules                   []string
	OutputBroker                string
	TaskConnectionString        string
	HttpPort                    int
	KafkaProducerConfiguration  []runtime.Configuration[*runtime_sarama.Producer]
	RabbitProducerConfiguration []runtime.Configuration[*runtime_rabbit.Producer]
	RetryConfiguration          []runtime.Configuration[*runtime_retry.Retry]
	RouteConfiguration          []runtime.Configuration[*runtime_bunrouter.Router]
}

func (c CronConfiguration[OK, OV, T]) Register() {
	for _, schedule := range c.Schedules {
		RegisterCronConfig(runtime_cron.WithCronJob(schedule, c.Scheduler, c.OutputTopic, c.TaskChannel))
	}
}

func (c CronConfiguration[OK, OV, T]) RegisterRuntime() {
	flows.RegisterRetry(
		c.RetryConfiguration,
	)
	RegisterProducerConfig(
		runtime_rabbit.WithProducerName(c.Name),
		runtime_rabbit.WithProducerConnectionString(c.TaskConnectionString),
	)
	RegisterProducerConfig(c.RabbitProducerConfiguration...)
	RegisterProducer()
	flows.RegisterProducer(
		c.OutputBroker,
		c.KafkaProducerConfiguration,
	)
	flows.RegisterRoute(
		c.HttpPort,
		c.RouteConfiguration,
	)
	RegisterCronConfigDefault()
	RegisterCron()
}

func (c CronConfiguration[OK, OV, T]) Runtime() runtime.Runtime {
	c.RegisterRuntime()
	c.Register()

	return &flows.RuntimeFacade{
		Runtimes: flows.InjectedRuntimes(),
	}
}
