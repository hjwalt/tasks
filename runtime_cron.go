package tasks

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/runway/inverse"
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

func (c CronConfiguration[OK, OV, T]) Register(ci inverse.Container) {
	flows.RegisterRetry(
		ci,
		c.RetryConfiguration,
	)
	RegisterProducer(
		ci,
		c.Name,
		c.TaskConnectionString,
		c.RabbitProducerConfiguration,
	)
	flows.RegisterProducer(
		ci,
		c.OutputBroker,
		c.KafkaProducerConfiguration,
	)
	flows.RegisterRoute(
		ci,
		c.HttpPort,
		c.RouteConfiguration,
	)
	RegisterCron(
		ci,
		[]runtime.Configuration[*runtime_cron.Cron]{},
	)

	// ADDING CRON AFTER CONFIG
	// Moving this above the cron config will result in NPE

	for _, schedule := range c.Schedules {
		RegisterCronConfig(ci, runtime_cron.WithCronJob(schedule, c.Scheduler, c.OutputTopic, c.TaskChannel))
	}
}
