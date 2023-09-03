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
	"github.com/hjwalt/runway/inverse"
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

func (c FlowConfiguration[IK, IV, OK, OV, T]) Register(ci inverse.Container) {
	flows.RegisterConsumerFunctionInjector(
		func(ctx context.Context, ci inverse.Container) (flows.ConsumerFunction, error) {
			taskProducer, err := GetRabbitProducer(ctx, ci)
			if err != nil {
				return flows.ConsumerFunction{}, err
			}

			wrappedBatch := task_flow.NewTaskFlow[IK, IV, OK, OV, T](
				task_flow.WithFlowFunction[IK, IV, OK, OV, T](c.Function),
				task_flow.WithProducer[IK, IV, OK, OV, T](taskProducer),
				task_flow.WithInputTopic[IK, IV, OK, OV, T](c.InputTopic),
				task_flow.WithOutputTopic[IK, IV, OK, OV, T](c.OutputTopic),
				task_flow.WithTaskChannel[IK, IV, OK, OV, T](c.TaskChannel),
			)

			return flows.ConsumerFunction{
				Topic: c.InputTopic.Name(),
				Key:   stateless.Base64PersistenceId,
				Fn:    wrappedBatch,
			}, nil
		},
		ci,
	)
	flows.RegisterRouteConfig(
		ci,
		runtime_bunrouter.WithRouterFlow(
			router.WithFlowStatelessOneToOne(c.InputTopic, c.OutputTopic),
		),
	)

	// RUNTIME

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
	flows.RegisterProducer(
		ci,
		c.OutputBroker,
		c.KafkaProducerConfiguration,
	)
	flows.RegisterConsumer(
		ci,
		c.Name,
		c.InputBroker,
		c.KafkaConsumerConfiguration,
	)
	flows.RegisterRoute(
		ci,
		c.HttpPort,
		c.RouteConfiguration,
	)
}
