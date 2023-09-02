package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/runtime_bun"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/flows/runtime_sarama"
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

func (c ExecutorBunFlowConfiguration[OK, OV, T]) Register() {
	RegisterConsumerExecutor(func(ctx context.Context) (task.Executor[structure.Bytes], error) {
		retry, err := flows.GetRetry(ctx)
		if err != nil {
			return nil, err
		}
		bunConnection, err := flows.GetPostgresqlConnection(ctx)
		if err != nil {
			return nil, err
		}
		flowProducer, err := flows.GetKafkaProducer(ctx)
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
	})
}

func (c ExecutorBunFlowConfiguration[OK, OV, T]) RegisterRuntime() {
	flows.RegisterPostgresql(
		c.Name,
		c.PostgresConnectionString,
		c.PostgresqlConfiguration,
	)
	flows.RegisterRetry(
		c.RetryConfiguration,
	)
	flows.RegisterProducer(
		c.OutputBroker,
		c.KafkaProducerConfiguration,
	)
	flows.RegisterRoute(
		c.HttpPort,
		c.RouteConfiguration,
	)
	RegisterConsumerConfig(
		runtime_rabbit.WithConsumerName(c.Name),
		runtime_rabbit.WithConsumerQueueName(c.TaskChannel.Name()),
		runtime_rabbit.WithConsumerConnectionString(c.TaskConnectionString),
	)
	RegisterConsumerConfig(c.RabbitConsumerConfiguration...)
	RegisterConsumer()
}

func (c ExecutorBunFlowConfiguration[OK, OV, T]) Runtime() runtime.Runtime {
	c.RegisterRuntime()
	c.Register()

	return &flows.RuntimeFacade{
		Runtimes: flows.InjectedRuntimes(),
	}
}
