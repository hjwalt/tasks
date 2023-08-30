package tasks

import (
	"context"

	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/router"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/flows/stateless"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_rabbit"
	"github.com/hjwalt/tasks/task"
	"github.com/hjwalt/tasks/task_flow"
)

type FlowConfiguration[IK any, IV any, OK any, OV any, T any] struct {
	Name                        string
	InputTopic                  flow.Topic[IK, IV]
	OutputTopic                 flow.Topic[OK, OV]
	TaskChannel                 task.Channel[T]
	Function                    task_flow.FlowFunction[IK, IV, OK, OV, T]
	InputBroker                 string
	OutputBroker                string
	TaskConnectionString        string
	HttpPort                    int
	KafkaProducerConfiguration  []runtime.Configuration[*runtime_sarama.Producer]
	KafkaConsumerConfiguration  []runtime.Configuration[*runtime_sarama.Consumer]
	RabbitProducerConfiguration []runtime.Configuration[*runtime_rabbit.Producer]
	RetryConfiguration          []runtime.Configuration[*runtime_retry.Retry]
	RouteConfiguration          []runtime.Configuration[*runtime_bunrouter.Router]
}

func (c FlowConfiguration[IK, IV, OK, OV, T]) Register() {
	flows.RegisterConsumerConfig(
		runtime_sarama.WithConsumerBroker(c.InputBroker),
		runtime_sarama.WithConsumerTopic(c.InputTopic.Name()),
		runtime_sarama.WithConsumerGroupName(c.Name),
	)
	flows.RegisterProducerConfig(
		runtime_sarama.WithProducerBroker(c.OutputBroker),
	)
	flows.RegisterRouteConfig(
		runtime_bunrouter.WithRouterPort(c.HttpPort),
		runtime_bunrouter.WithRouterFlow(
			router.WithFlowStatelessOneToOne(c.InputTopic, c.OutputTopic),
		),
	)
	RegisterProducerConfig(
		runtime_rabbit.WithProducerName(c.Name),
		runtime_rabbit.WithProducerConnectionString(c.TaskConnectionString),
	)

	RegisterProducerConfig(c.RabbitProducerConfiguration...)
	RegisterProducer()
	flows.RegisterRetry(c.RetryConfiguration)
	flows.RegisterProducerConfig(c.KafkaProducerConfiguration...)
	flows.RegisterProducer()
	flows.RegisterConsumerConfig(c.KafkaConsumerConfiguration...)
	flows.RegisterConsumerKeyedConfig()
	flows.RegisterConsumer()
	flows.RegisterRouteConfigDefault()
	flows.RegisterRouteConfig(c.RouteConfiguration...)
	flows.RegisterRoute()
	flows.RegisterConsumerKeyedKeyFunction(stateless.Base64PersistenceId)
	flows.RegisterConsumerKeyedFunction(func(ctx context.Context) (stateless.BatchFunction, error) {
		retry, err := flows.GetRetry(ctx)
		if err != nil {
			return nil, err
		}
		flowProducer, err := flows.GetKafkaProducer(ctx)
		if err != nil {
			return nil, err
		}
		taskProducer, err := GetRabbitProducer(ctx)
		if err != nil {
			return nil, err
		}

		wrappedBatch := task_flow.NewTaskFlow[IK, IV, OK, OV, T](
			task_flow.WithFlowFunction[IK, IV, OK, OV, T](c.Function),
			task_flow.WithProducer[IK, IV, OK, OV, T](taskProducer),
			task_flow.WithInputTopic[IK, IV, OK, OV, T](c.InputTopic),
			task_flow.WithOutputTopic[IK, IV, OK, OV, T](c.OutputTopic),
			task_flow.WithTaskChannel[IK, IV, OK, OV, T](c.TaskChannel),
		)

		wrappedBatch = stateless.NewProducerBatchFunction(
			stateless.WithBatchProducerNextFunction(wrappedBatch),
			stateless.WithBatchProducerRuntime(flowProducer),
			stateless.WithBatchProducerPrometheus(),
		)

		wrappedBatch = stateless.NewBatchRetry(
			stateless.WithBatchRetryNextFunction(wrappedBatch),
			stateless.WithBatchRetryRuntime(retry),
			stateless.WithBatchRetryPrometheus(),
		)

		return wrappedBatch, nil
	})
}

func (c FlowConfiguration[IK, IV, OK, OV, T]) Runtime() runtime.Runtime {
	c.Register()

	return &flows.RuntimeFacade{
		Runtimes: flows.InjectedRuntimes(),
	}
}
