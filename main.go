package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"go_kinesis_es_project/engine"
	"go_kinesis_es_project/scheduler"
	"go_kinesis_es_project/test_event/processor"
)

func init() {
	env := godotenv.Load() //Load .env file
	if env != nil {
		fmt.Print(env)
	}
}

func main() {
	e := engine.ConcurrentEngine{
		Scheduler:   &scheduler.QueuedScheduler{},
		WorkerCount: 10,
	}

	e.Run(engine.Request{Processor: processor.ProcessTestEvent})
}
