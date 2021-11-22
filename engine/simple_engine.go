package engine

import (
	"go_kinesis_es_project/TestEvent/processor"
	"go_kinesis_es_project/fetcher"
	"go_kinesis_es_project/models"
	"log"
	"time"
)

type SimpleEngine struct {
}

var events []interface{}
var awsConfig = models.AwsConfig{}

func (e SimpleEngine) Run() {

	request := Request{
		StreamName: "TestStream",
		Processor:  processor.ProcessTestEvent,
	}

	for {
		processResult, err := worker(request)
		if err != nil {
			log.Printf("Fetcher: error processing from message from stream %s: %v", request.StreamName, err)
			time.Sleep(1000)
			continue
		}
		for _, item := range processResult.Items {

			log.Printf("Got Item: %v", item)
		}
		events = append(events, processResult.Items...)
	}
}

func worker(r Request) (ProcessResult, error) {
	contents, err := fetcher.Fetch(awsConfig)
	if err != nil {
		log.Printf("Fetcher: error fetching from stream %s: %v", r.StreamName, err)
		time.Sleep(1000)
		return ProcessResult{}, err
	}
	processResult, err := r.Processor(contents)

	return processResult, nil
}
