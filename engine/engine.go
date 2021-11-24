package engine

import (
	"log"
)

type ConcurrentEngine struct {
	Fetcher     EventBusFetcher
	Scheduler   Scheduler
	WorkerCount int
}

func (e *ConcurrentEngine) Run() {
	// Initiate scheduler.
	resultChan := make(chan ProcessResult)
	// Start the scheduler, init those two chan.
	e.Scheduler.Run()
	// Setup workers into queue.
	for i := 0; i < e.WorkerCount; i++ {
		createWorker(e.Scheduler.WorkerChan(), resultChan, e.Scheduler)
	}

	requestChan := make(chan Request)
	e.Fetcher.Run(requestChan)

	for {
		// 1. run fetcher, to get message and submit request to scheduler.
		request := <-requestChan
		e.Scheduler.Submit(request)

		// 2. listen scheduler `out` chan, and print out the result.
		result := <-resultChan
		for _, item := range result.Items {
			log.Printf("Processed item: %v", item)
			// todo: add items into pool to send to ES.
		}
	}
}

func createWorker(inChan chan Request, outChan chan ProcessResult, ready ReadyNotifier) {
	go func() {
		for {
			// tell scheduler this worker is ready.
			ready.WorkerReady(inChan)
			request := <-inChan // Scheduler choose this worker, then send request.
			result, err := worker(request)
			if err != nil {
				continue
			}
			outChan <- result
		}
	}()
}
