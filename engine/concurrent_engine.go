package engine

import (
	"log"
)

type ConcurrentEngine struct {
	Scheduler   Scheduler
	WorkerCount int
}

type Scheduler interface {
	Submit(Request)
	ConfigureMasterWorkerChan(chan Request)
}

func (e *ConcurrentEngine) Run(seeds ...Request) {
	in := make(chan Request)
	out := make(chan ProcessResult)
	e.Scheduler.ConfigureMasterWorkerChan(in)

	for _, r := range seeds {
		e.Scheduler.Submit(r)
	}

	for i := 0; i < e.WorkerCount; i++ {
		createWorker(in, out)
	}

	itemCount := 0
	for {
		result := <-out

		for _, item := range result.Items {
			log.Printf("Got item: %v", item)
			// todo: add items into pool to send to ES.
			itemCount++
		}
	}
}

func createWorker(in chan Request, out chan ProcessResult) {
	go func() {
		for {
			request := <-in
			result, err := worker(request)
			if err != nil {
				continue
			}

			out <- result
		}
	}()
}
