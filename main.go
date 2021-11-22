package main

import (
	"go_kinesis_es_project/test_event/processor"
	"go_kinesis_es_project/engine"
	"go_kinesis_es_project/scheduler"
)

func main() {
	e := engine.ConcurrentEngine{
		Scheduler:   &scheduler.SimpleScheduler{},
		WorkerCount: 10,
	}

	e.Run(
		engine.Request{
			StreamName: "TestStream",
			Processor:  processor.ProcessTestEvent,
		})
}
