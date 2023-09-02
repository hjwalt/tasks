package tasks

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/flows/flow"
	"github.com/hjwalt/flows/router"
	"github.com/hjwalt/flows/runtime_bunrouter"
	"github.com/hjwalt/flows/runtime_retry"
	"github.com/hjwalt/flows/runtime_sarama"
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
	RegisterTaskFlowFunction[IK, IV, OK, OV, T](
		c.Function,
		c.InputTopic,
		c.OutputTopic,
		c.TaskChannel,
	)
	flows.RegisterRouteConfig(
		runtime_bunrouter.WithRouterFlow(
			router.WithFlowStatelessOneToOne(c.InputTopic, c.OutputTopic),
		),
	)
}

func (c FlowConfiguration[IK, IV, OK, OV, T]) RegisterRuntime() {
	flows.RegisterRetry(
		c.RetryConfiguration,
	)
	RegisterProducerConfig(
		runtime_rabbit.WithProducerName(c.Name),
		runtime_rabbit.WithProducerConnectionString(c.TaskConnectionString),
	)
	RegisterProducerConfig(c.RabbitProducerConfiguration...)
	RegisterProducer()
	flows.RegisterProducer(
		c.OutputBroker,
		c.KafkaProducerConfiguration,
	)
	flows.RegisterConsumer(
		c.Name,
		c.InputBroker,
		c.KafkaConsumerConfiguration,
	)
	flows.RegisterRoute(
		c.HttpPort,
		c.RouteConfiguration,
	)
}

func (c FlowConfiguration[IK, IV, OK, OV, T]) Runtime() runtime.Runtime {
	c.RegisterRuntime()
	c.Register()

	return &flows.RuntimeFacade{
		Runtimes: flows.InjectedRuntimes(),
	}
}
