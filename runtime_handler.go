package tasks

import (
	"github.com/hjwalt/flows/runtime_sarama"
	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/tasks/runtime_rabbit"
)

type HandlerConfiguration struct {
	KafkaProducerConfiguration  []runtime.Configuration[*runtime_sarama.Producer]
	RabbitConsumerConfiguration []runtime.Configuration[*runtime_rabbit.Consumer]
}
