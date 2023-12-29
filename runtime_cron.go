package tasks

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_cron"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
)

type CronConfiguration[OK any, OV any, T any] struct {
	Name                        string
	TaskChannel                 task.Channel[T]
	Scheduler                   task.Scheduler[OK, OV, T]
	Schedules                   []string
	TaskConnectionString        string
	HttpPort                    int
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
		RegisterCronConfig(ci, runtime_cron.WithCronJob(schedule, c.Scheduler, c.TaskChannel))
	}
}
