package example_task_executor

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/runtime_bun"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
	"go.uber.org/zap"
)

func fn(c context.Context, t task.Message[string], bun runtime_bun.BunConnection, flowProducer flow.Producer) (flow.Message[string, string], error) {
	logger.Info("handling", zap.String("body", t.Value))
	return flow.Message[string, string]{
		Topic: "word-updated",
		Key:   t.Value,
		Value: t.Value + " completed",
	}, nil
}

func instance() runtime.Runtime {
	r := tasks.ExecutorBunFlowConfiguration[string, string, string]{
		Name:                     Instance,
		TaskChannel:              task.StringChannel("tasks"),
		Executor:                 fn,
		OutputTopic:              flow.StringTopic("word-updated"),
		OutputBroker:             "localhost:9092",
		TaskConnectionString:     "amqp://guest:guest@localhost:5672/",
		PostgresConnectionString: "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
		HttpPort:                 8081,
		RabbitConsumerConfiguration: []runtime.Configuration[*runtime_rabbit.Consumer]{
			runtime_rabbit.WithConsumerQueueDurable(false),
		},
	}
	return r.Runtime()
}

const (
	Instance = "tasks-example-executor"
)

func Register(m flows.Main) {
	err := m.Register(Instance, instance)
	if err != nil {
		panic(err)
	}
}
