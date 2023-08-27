package runtime_rabbit

import (
	"context"
	"errors"
	"time"

	"github.com/hjwalt/runway/runtime"
	"github.com/hjwalt/runway/structure"
	"github.com/hjwalt/tasks/task"
	"github.com/rabbitmq/amqp091-go"
)

// constructor
var NewProducer = runtime.ConstructorFor[*Producer, task.Producer](
	func() *Producer {
		return &Producer{
			Name:         "tasks",
			QueueDurable: true,
		}
	},
	func(hr *Producer) task.Producer {
		return hr
	},
)

// configuration
func WithProducerName(name string) runtime.Configuration[*Producer] {
	return func(p *Producer) *Producer {
		p.Name = name
		return p
	}
}

func WithProducerConnectionString(connectionString string) runtime.Configuration[*Producer] {
	return func(p *Producer) *Producer {
		p.ConnectionString = connectionString
		return p
	}
}

func WithProducerQueueName(queueName string) runtime.Configuration[*Producer] {
	return func(p *Producer) *Producer {
		p.QueueName = queueName
		return p
	}
}

func WithProducerQueueDurable(durable bool) runtime.Configuration[*Producer] {
	return func(p *Producer) *Producer {
		p.QueueDurable = durable
		return p
	}
}

// implementation
type Producer struct {
	Name             string
	ConnectionString string // amqp://guest:guest@localhost:5672/
	QueueName        string
	QueueDurable     bool

	connection *amqp091.Connection
	channel    *amqp091.Channel
	queue      *amqp091.Queue
}

func (p *Producer) Start() error {
	config := amqp091.Config{
		Heartbeat: 10 * time.Second,
		Locale:    "en_US",
		Properties: amqp091.Table{
			"connection_name": p.Name,
		},
	}

	if conn, err := amqp091.DialConfig(p.ConnectionString, config); err != nil {
		return errors.Join(err, ErrRabbitConnection)
	} else {
		p.connection = conn
	}

	if ch, err := p.connection.Channel(); err != nil {
		return errors.Join(err, ErrRabbitChannel)
	} else {
		p.channel = ch
	}

	if err := p.channel.Confirm(false); err != nil {
		return errors.Join(err, ErrRabbitConfirmMode)
	}

	if q, err := p.channel.QueueDeclare(p.QueueName, p.QueueDurable, false, false, false, nil); err != nil {
		return errors.Join(err, ErrRabbitQueue)
	} else {
		p.queue = &q
	}

	return nil
}

func (p *Producer) Stop() {
	p.channel.Close()
	p.connection.Close()
}

func (p *Producer) Produce(c context.Context, t task.Message[structure.Bytes]) error {
	ctx, cancel := context.WithTimeout(c, 5*time.Second)
	defer cancel()

	confirm, err := p.channel.PublishWithDeferredConfirmWithContext(ctx,
		"",           // exchange
		p.queue.Name, // routing key
		false,        // mandatory
		false,        // immediate
		amqp091.Publishing{
			DeliveryMode: amqp091.Persistent,
			Headers:      t.Headers,
			ContentType:  "application/octet-stream",
			Body:         t.Value,
		},
	)
	if err != nil {
		return errors.Join(err, ErrRabbitProduce)
	}

	published, err := confirm.WaitContext(ctx)
	if err != nil {
		return errors.Join(err, ErrRabbitProduce)
	}

	if !published {
		return errors.Join(err, ErrRabbitProduce)
	}

	return nil
}

var (
	ErrRabbitConnection          = errors.New("rabbitmq connection failed")
	ErrRabbitChannel             = errors.New("rabbitmq channel failed")
	ErrRabbitQueue               = errors.New("rabbitmq queue declaration failed")
	ErrRabbitProduce             = errors.New("rabbitmq producing")
	ErrRabbitProduceNotConfirmed = errors.New("rabbitmq produce not confirmed")
	ErrRabbitConfirmMode         = errors.New("rabbitmq channel confirm mode failed")
)
