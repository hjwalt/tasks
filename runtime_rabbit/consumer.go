package runtime_rabbit

import (
	"context"
	"errors"

	"github.com/hjwalt/flows/message"
	"github.com/hjwalt/runway/logger"
	"github.com/hjwalt/tasks/task"
	"github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Consumer struct {
	ConnectionString string // amqp://guest:guest@localhost:5672/
	QueueName        string
	QueueDurable     bool
	Handler          task.HandlerFunction

	connection *amqp091.Connection
	channel    *amqp091.Channel
	queue      *amqp091.Queue
	messages   <-chan amqp091.Delivery
}

func (r *Consumer) Start() error {
	if conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/"); err != nil {
		return errors.Join(err, ErrRabbitConnection)
	} else {
		r.connection = conn
	}

	if ch, err := r.connection.Channel(); err != nil {
		return errors.Join(err, ErrRabbitChannel)
	} else {
		r.channel = ch
	}

	if err := r.channel.Qos(1, 0, false); err != nil {
		return errors.Join(err, ErrRabbitPrefetch)
	}

	if q, err := r.channel.QueueDeclare(r.QueueName, r.QueueDurable, false, false, false, nil); err != nil {
		return errors.Join(err, ErrRabbitQueue)
	} else {
		r.queue = &q
	}

	if msgs, err := r.channel.Consume(
		r.queue.Name, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	); err != nil {
		return errors.Join(err, ErrRabbitChannel)
	} else {
		r.messages = msgs
	}

	return nil
}

func (r *Consumer) Stop() {
	r.channel.Close()
	r.connection.Close()
}

func (r *Consumer) Loop(ctx context.Context, cancel context.CancelFunc) error {

	m, closed := <-r.messages
	if closed {
		cancel()
		return nil
	}

	t := task.Task[message.Bytes]{
		Value:     m.Body,
		Headers:   m.Headers,
		Timestamp: m.Timestamp,
	}

	_, err := r.Handler(ctx, t)
	if err != nil {
		return errors.Join(err, ErrRabbitConsume)
	}

	logger.Info("message", zap.String("body", string(m.Body)))

	m.Ack(false)

	return nil
}

var (
	ErrRabbitPrefetch = errors.New("rabbit prefetch setting")
	ErrRabbitMessages = errors.New("rabbit messages start consume")
	ErrRabbitConsume  = errors.New("rabbit messages consume")
)
