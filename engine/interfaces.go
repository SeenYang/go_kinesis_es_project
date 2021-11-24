package engine

import (
	"go_kinesis_es_project/fetcher/models"
)

type Scheduler interface {
	ReadyNotifier
	Submit(Request)
	WorkerChan() chan Request
	Run()
}

type ReadyNotifier interface {
	WorkerReady(chan Request)
}

type EventBusFetcher interface {
	Fetch(*models.AwsConfig) ([][]byte, error)
	Run(chan Request)
}
