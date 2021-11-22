package engine

import (
	"go_kinesis_es_project/fetcher"
	"log"
	"time"
)

func worker(r Request) (ProcessResult, error) {
	contents, err := fetcher.Fetch()
	if err != nil {
		log.Printf("Fetcher: error fetching event: %v", err)
		time.Sleep(1000)
		return ProcessResult{}, err
	}
	processResult, err := r.Processor(contents)

	return processResult, nil
}
