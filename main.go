package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"go_kinesis_es_project/engine"
	"go_kinesis_es_project/fetcher"
	"go_kinesis_es_project/scheduler"
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
		Fetcher:     &fetcher.KinesisFetcher{},
		WorkerCount: 10,
	}

	e.Run()
}
