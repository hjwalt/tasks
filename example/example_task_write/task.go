package example_task_write

import (
	"github.com/hjwalt/flows"
	"github.com/hjwalt/runway/runtime"
)

func instance() runtime.Runtime {
	return nil
}

func Register(m flows.Main) {
	err := m.Register("task_write", instance)
	if err != nil {
		panic(err)
	}
}
