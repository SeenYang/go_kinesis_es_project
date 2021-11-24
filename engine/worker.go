package engine

import (
	"log"
	"time"
)

func worker(r Request) (ProcessResult, error) {
	processResult, err := r.Processor(r.Contents)
	if err != nil {
		log.Printf("Worker: error process event: %v", err)
		time.Sleep(1000)
		return ProcessResult{}, err
	}
	return processResult, nil
}
