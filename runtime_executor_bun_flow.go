package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/runtime_bun"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/runway/inverse"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
	"github.com/hjwalt/tasks/task_executor_bun_flow"
	"github.com/hjwalt/tasks/task_executor_retry"
)

type ExecutorBunFlowConfiguration[OK any, OV any, T any] struct {
	Name                        string
	TaskChannel                 task.Channel[T]
	Executor                    task_executor_bun_flow.Executor[OK, OV, T]
	OutputTopic                 flow.Topic[OK, OV]
	OutputBroker                string
	TaskConnectionString        string
	HttpPort                    int
	PostgresConnectionString    string
	PostgresqlConfiguration     []runtime.Configuration[*runtime_bun.PostgresqlConnection]
	KafkaProducerConfiguration  []runtime.Configuration[*runtime_sarama.Producer]
	RabbitConsumerConfiguration []runtime.Configuration[*runtime_rabbit.Consumer]
	RetryConfiguration          []runtime.Configuration[*runtime_retry.Retry]
	RouteConfiguration          []runtime.Configuration[*runtime_bunrouter.Router]
}

func (c ExecutorBunFlowConfiguration[OK, OV, T]) Register(ci inverse.Container) {
	RegisterConsumerExecutor(
		ci,
		func(ctx context.Context, ci inverse.Container) (task.Executor[structure.Bytes], error) {
			retry, err := flows.GetRetry(ctx, ci)
			if err != nil {
				return nil, err
			}
			bunConnection, err := flows.GetPostgresqlConnection(ctx, ci)
			if err != nil {
				return nil, err
			}
			flowProducer, err := flows.GetKafkaProducer(ctx, ci)
			if err != nil {
				return nil, err
			}

			executor := task_executor_bun_flow.NewTaskExecutor[OK, OV, T](
				task_executor_bun_flow.WithBunConnection[OK, OV, T](bunConnection),
				task_executor_bun_flow.WithProducer[OK, OV, T](flowProducer),
				task_executor_bun_flow.WithExecutor[OK, OV, T](c.Executor),
				task_executor_bun_flow.WithOutputTopic[OK, OV, T](c.OutputTopic),
				task_executor_bun_flow.WithTaskChannel[OK, OV, T](c.TaskChannel),
			)

			executor = task_executor_retry.New(
				task_executor_retry.WithRetry(retry),
				task_executor_retry.WithExecutor(executor),
			)

			return executor, nil
		},
	)

	// RUNTIME

	flows.RegisterPostgresql(
		ci,
		c.Name,
		c.PostgresConnectionString,
		c.PostgresqlConfiguration,
	)
	flows.RegisterRetry(
		ci,
		c.RetryConfiguration,
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
	RegisterConsumer(
		ci,
		c.Name,
		c.TaskConnectionString,
		c.TaskChannel.Name(),
		c.RabbitConsumerConfiguration,
	)
}
