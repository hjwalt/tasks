package runtime_cron

import (
	"context"

	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/runway/format"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/task"
)

type Job[OK any, OV any, T any] struct {
	flowProducer flow.Producer
	taskProducer task.Producer
	scheduler    task.Scheduler[OK, OV, T]
	topic        flow.Topic[OK, OV]
	channel      task.Channel[T]
}

func (j *Job[OK, OV, T]) Run() {
	ctx := context.Background()

	t, m, err := j.scheduler(ctx)
	if err != nil {
		logger.ErrorErr("scheduler failed with error", err)
		return
	}

	mb, mberr := flow.Convert(m, j.topic.KeyFormat(), j.topic.ValueFormat(), format.Bytes(), format.Bytes())
	if mberr != nil {
		logger.ErrorErr("scheduler failed to convert flow message", mberr)
		return
	}
	mb.Topic = j.topic.Name()

	tb, tberr := task.Convert(t, j.channel.ValueFormat(), format.Bytes())
	if tberr != nil {
		logger.ErrorErr("scheduler failed to convert task message", tberr)
		return
	}
	tb.Channel = j.channel.Name()

	taskProduceErr := j.taskProducer.Produce(ctx, tb)
	if taskProduceErr != nil {
		logger.ErrorErr("scheduler failed to publish task", taskProduceErr)
		return
	}

	flowProduceErr := j.flowProducer.Produce(ctx, []flow.Message[structure.Bytes, structure.Bytes]{mb})
	if flowProduceErr != nil {
		logger.ErrorErr("scheduler failed to publish message", flowProduceErr)
		return
	}
}
