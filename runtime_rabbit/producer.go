package runtime_rabbit

import (
	"context"
	"errors"
	"time"

	"github.com/hjwalt/flows/message"
	"github.com/hjwalt/tasks/task"
	"github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	ConnectionString string // amqp://guest:guest@localhost:5672/
	QueueName        string
	QueueDurable     bool

	connection *amqp091.Connection
	channel    *amqp091.Channel
	queue      *amqp091.Queue
}

func (p *Producer) Start() error {
	if conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/"); err != nil {
		return errors.Join(err, ErrRabbitConnection)
	} else {
		p.connection = conn
	}

	if ch, err := p.connection.Channel(); err != nil {
		return errors.Join(err, ErrRabbitChannel)
	} else {
		p.channel = ch
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

func (p *Producer) Produce(c context.Context, t task.Task[message.Bytes]) error {
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
			ContentType:  "application/octed-stream",
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
)
