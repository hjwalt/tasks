package main

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/runway/environment"
	"github.com/hjwalt/tasks/example/example_task_cron"
	"github.com/hjwalt/tasks/example/example_task_executor"
	"github.com/hjwalt/tasks/example/example_task_flow"
)

func main() {
	m := flows.NewMain()

	example_task_cron.Register(m)
	example_task_flow.Register(m)
	example_task_executor.Register(m)

	err := m.Start(environment.GetString("INSTANCE", example_task_cron.Instance))

	if err != nil {
		panic(err)
	}
}
