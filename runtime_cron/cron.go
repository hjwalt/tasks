package runtime_cron

import (
	"time"

	"github.com/hjwalt/flows/flow"
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
func WithFlowProducer(flowProducer flow.Producer) runtime.Configuration[*Cron] {
	return func(c *Cron) *Cron {
		c.flowProducer = flowProducer
		return c
	}
}

func WithTaskProducer(taskProducer task.Producer) runtime.Configuration[*Cron] {
	return func(c *Cron) *Cron {
		c.taskProducer = taskProducer
		return c
	}
}

func WithCronJob[OK any, OV any, T any](
	schedule string,
	scheduler task.Scheduler[OK, OV, T],
	topic flow.Topic[OK, OV],
	channel task.Channel[T],
) runtime.Configuration[*Cron] {
	return func(c *Cron) *Cron {
		c.cron.AddJob(
			schedule,
			&Job[OK, OV, T]{
				flowProducer: c.flowProducer,
				taskProducer: c.taskProducer,
				scheduler:    scheduler,
				topic:        topic,
				channel:      channel,
			},
		)
		return c
	}
}

// implementation
type Cron struct {
	flowProducer flow.Producer
	taskProducer task.Producer
	cron         *cron.Cron
}

func (c *Cron) Start() error {
	logger.Info("cron starting")
	c.cron.Start()
	return nil
}

func (c *Cron) Stop() {
	logger.Info("cron stopping")
	c.cron.Stop()
}
