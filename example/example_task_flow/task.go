package example_task_flow

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/tasks"
	"github.com/hjwalt/tasks/task"
	"go.uber.org/zap"
)

func fn(c context.Context, m flow.Message[string, string]) (task.Message[string], flow.Message[string, string], error) {

	logger.Info("count", zap.String("remap", m.Value+" updated"), zap.String("key", m.Key))
	return task.Message[string]{
			Value: m.Value,
		},
		flow.Message[string, string]{
			Topic:   "word-updated",
			Key:     m.Key,
			Value:   m.Value + " published",
			Headers: m.Headers,
		}, nil
}

func Registrar(ci inverse.Container) flows.Prebuilt {
	return tasks.FlowConfiguration[string, string, string, string, string]{
		Name:                 Instance,
		Function:             fn,
		InputTopic:           flow.StringTopic("word"),
		OutputTopic:          flow.StringTopic("word-updated"),
		TaskChannel:          task.StringChannel("tasks"),
		InputBroker:          "localhost:9092",
		OutputBroker:         "localhost:9092",
		TaskConnectionString: "amqp://guest:guest@localhost:5672/",
		HttpPort:             8082,
	}
}

const (
	Instance = "tasks-example-flow"
)

func Register(m flows.Main) {
	m.Prebuilt(Instance, Registrar)
}
