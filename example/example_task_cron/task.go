package example_task_cron

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks"
	"github.com/hjwalt/tasks/task"
)

func fn(c context.Context) (task.Message[string], flow.Message[string, string], error) {
	logger.Info("cron")

	return task.Message[string]{
			Value: "cron",
		},
		flow.Message[string, string]{
			Topic: "word-updated",
			Key:   "cron",
			Value: "cron published",
		}, nil
}

func instance() runtime.Runtime {
	r := tasks.CronConfiguration[string, string, string]{
		Name:        Instance,
		OutputTopic: flow.StringTopic("cron-scheduled"),
		TaskChannel: task.StringChannel("tasks"),
		Scheduler:   fn,
		Schedules: []string{
			"0 * * * * *",
			"@every 2s",
		},
		OutputBroker:         "localhost:9092",
		TaskConnectionString: "amqp://guest:guest@localhost:5672/",
		HttpPort:             8082,
	}
	return r.Runtime()
}

const (
	Instance = "tasks-example-cron"
)

func Register(m flows.Main) {
	err := m.Register(Instance, instance)
	if err != nil {
		panic(err)
	}
}
