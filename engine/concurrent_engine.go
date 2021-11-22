package engine

import (
	"log"
)

type ConcurrentEngine struct {
	Scheduler   Scheduler
	WorkerCount int
}

func (e *ConcurrentEngine) Run(request Request) {
	out := make(chan ProcessResult)
	// Start the scheduler, init those two chan.
	e.Scheduler.Run()

	for i := 0; i < e.WorkerCount; i++ {
		createWorker(e.Scheduler.WorkerChan(), out, e.Scheduler)
	}

	e.Scheduler.Submit(request)

	for {
		itemCount := 0
		result := <-out

		for _, item := range result.Items {
			log.Printf("Got item: %v", item)
			// todo: add items into pool to send to ES.
			itemCount++
		}
		log.Printf("Got %v item(s) in total", itemCount)
		e.Scheduler.Submit(request)
	}
}

func createWorker(in chan Request, out chan ProcessResult, ready ReadyNotifier) {
	go func() {
		for {
			// tell scheduler this worker is ready.
			ready.WorkerReady(in)
			request := <-in // Scheduler choose this worker, then send request.
			result, err := worker(request)
			if err != nil {
				continue
			}
			out <- result
		}
	}()
}
