package runtime_cron

import (
	"time"

	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/task"
	"github.com/robfig/cron/v3"
)

// constructor
var NewCron = runtime.ConstructorFor[*Cron, runtime.Runtime](
	func() *Cron {
		return &Cron{
			cron: cron.New(
				cron.WithLocation(time.UTC),
				cron.WithSeconds(),
			),
		}
	},
	func(hr *Cron) runtime.Runtime {
		return hr
	},
)

// configuration

func WithTaskProducer(taskProducer task.Producer) runtime.Configuration[*Cron] {
	return func(c *Cron) *Cron {
		c.taskProducer = taskProducer
		return c
	}
}

func WithCronJob[OK any, OV any, T any](
	schedule string,
	scheduler task.Scheduler[OK, OV, T],
	channel task.Channel[T],
) runtime.Configuration[*Cron] {
	return func(c *Cron) *Cron {
		c.cron.AddJob(
			schedule,
			&Job[OK, OV, T]{
				taskProducer: c.taskProducer,
				scheduler:    scheduler,
				channel:      channel,
			},
		)
		return c
	}
}

// implementation
type Cron struct {
	taskProducer task.Producer
	cron         *cron.Cron
}

func (c *Cron) Start() error {
	logger.Debug("cron starting")
	c.cron.Start()
	return nil
}

func (c *Cron) Stop() {
	logger.Debug("cron stopping")
	c.cron.Stop()
}
