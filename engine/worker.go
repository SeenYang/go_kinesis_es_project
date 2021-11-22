package engine

import (
	"go_kinesis_es_project/fetcher"
	"log"
	"time"
)

func worker(r Request) (ProcessResult, error) {

	awsConfig := AwsConfig{}

	contents, err := fetcher.Fetch(awsConfig)
	if err != nil {
		log.Printf("Fetcher: error fetching from stream %s: %v", r.StreamName, err)
		time.Sleep(1000)
		return ProcessResult{}, err
	}
	processResult, err := r.Processor(contents)

	return processResult, nil
}
