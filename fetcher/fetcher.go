package fetcher

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"go_kinesis_es_project/engine"
	"go_kinesis_es_project/fetcher/models"
	"go_kinesis_es_project/test_event/processor"
	"log"
	"os"
	"time"
)

var (
	awsConfig models.AwsConfig
)

type KinesisFetcher struct{}

func (k *KinesisFetcher) Run(requests chan engine.Request) {
	awsConfig := models.AwsConfig{
		Stream:          os.Getenv("KINESIS_STREAM_NAME"),
		Region:          os.Getenv("KINESIS_REGION"),
		Endpoint:        os.Getenv("AWS_ENDPOINT"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}

	s := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(awsConfig.Region),
		Endpoint:    aws.String(awsConfig.Endpoint),
		Credentials: credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, awsConfig.SessionToken),
	}))
	kc := kinesis.New(s)
	streamName := aws.String(awsConfig.Stream)
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Printf("Fetcher: Error while fetching. %v", err)
	}
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard id is provided when making put record(s) request.
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		// ShardIteratorType: awsconfig.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: awsconfig.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		log.Printf("Fetcher: Error while fetching. %v", err)
	}

	shardIterator := iteratorOutput.ShardIterator

	var temp *string
	// get data using infinity looping
	// we will attempt to consume data every 1 secons, if no data, nothing will be happen
	go func() {

		for {
			// get records use shard iterator for making request
			records, err := kc.GetRecords(&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			})

			// if error, wait until 1 seconds and continue the looping process
			if err != nil {
				time.Sleep(1000 * time.Millisecond)
				continue
			}

			// process the data
			if len(records.Records) > 0 {
				for _, d := range records.Records {
					m := make(map[string]interface{})
					err := json.Unmarshal([]byte(d.Data), &m)
					if err != nil {
						log.Println(err)
						continue
					}
					log.Printf("GetRecords Data: %v\n", m)
					requests <- engine.Request{
						Processor: processor.ProcessTestEvent,
						Contents:  d.Data,
					}
				}
			} else if records.NextShardIterator == temp || shardIterator == records.NextShardIterator || err != nil {
				log.Printf("GetRecords ERROR: %v\n", err)
				break
			} else {
				log.Printf("Fetcher: No records from kinesis.")
			}
			shardIterator = records.NextShardIterator
			//time.Sleep(1000 * time.Millisecond)
		}
	}()
	//go func() {
	//	for {
	//		message, err := k.Fetch(&awsConfig)
	//		log.Printf("Fetcher: Get message: %v", message)
	//		if err != nil {
	//			continue
	//		}
	//
	//		requests <- engine.Request{
	//			Processor: processor.ProcessTestEvent,
	//			Contents:  message,
	//		}
	//
	//		time.Sleep(3000)
	//	}
	//}()

}

func (k *KinesisFetcher) Fetch(awsConfig *models.AwsConfig) ([][]byte, error) { // connect to aws config kinesis
	s := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(awsConfig.Region),
		Endpoint:    aws.String(awsConfig.Endpoint),
		Credentials: credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, awsConfig.SessionToken),
	}))
	kc := kinesis.New(s)
	streamName := aws.String(awsConfig.Stream)
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Printf("Fetcher: Error while fetching. %v", err)
		return nil, err
	}
	// retrieve iterator
	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
		// Shard id is provided when making put record(s) request.
		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		// ShardIteratorType: awsconfig.String("AT_SEQUENCE_NUMBER"),
		// ShardIteratorType: awsconfig.String("LATEST"),
		StreamName: streamName,
	})
	if err != nil {
		log.Printf("Fetcher: Error while fetching. %v", err)
		return nil, err
	}

	shardIterator := iteratorOutput.ShardIterator
	// get records use shard iterator for making request
	records, err := kc.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: shardIterator,
	})

	// if error, wait until 1 seconds and continue the looping process
	if err != nil {
		time.Sleep(40 * time.Millisecond)
		return nil, err
	}
	var messages [][]byte
	if len(records.Records) > 0 {
		for _, d := range records.Records {
			messages = append(messages, d.Data)
		}
	} else {
		log.Printf("Fetcher: No message obtained from kinesis.")
	}
	shardIterator = records.NextShardIterator
	return messages, nil
}

//func (k *KinesisFetcher) Run(requestChan chan engine.Request) {

//}

//func Fetch() ([][]byte, error) {
//	awsConfig := models.AwsConfig{
//		Stream:          os.Getenv("KINESIS_STREAM_NAME"),
//		Region:          os.Getenv("KINESIS_REGION"),
//		Endpoint:        os.Getenv("AWS_ENDPOINT"),
//		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
//		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
//		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
//	}
//
//	// connect to aws config kinesis
//	s := session.Must(session.NewSession(&aws.Config{
//		Region:      aws.String(awsConfig.Region),
//		Endpoint:    aws.String(awsConfig.Endpoint),
//		Credentials: credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, awsConfig.SessionToken),
//	}))
//
//
//	kc := kinesis.New(s)
//	streamName := aws.String(awsConfig.Stream)
//	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
//	if err != nil {
//		log.Panic(err)
//		return nil, err
//	}
//
//	// retrieve iterator
//	iteratorOutput, err := kc.GetShardIterator(&kinesis.GetShardIteratorInput{
//		// Shard id is provided when making put record(s) request.
//		ShardId:           aws.String(*streams.StreamDescription.Shards[0].ShardId),
//		ShardIteratorType: aws.String("TRIM_HORIZON"),
//		// ShardIteratorType: awsconfig.String("AT_SEQUENCE_NUMBER"),
//		// ShardIteratorType: awsconfig.String("LATEST"),
//		StreamName: streamName,
//	})
//	if err != nil {
//		log.Panic(err)
//		return nil, err
//	}
//
//	shardIterator := iteratorOutput.ShardIterator
//	// get records use shard iterator for making request
//	records, err := kc.GetRecords(&kinesis.GetRecordsInput{
//		ShardIterator: shardIterator,
//	})
//
//	// if error, wait until 1 seconds and continue the looping process
//	if err != nil {
//		return nil, err
//	}
//	var messages [][]byte
//	if len(records.Records) > 0 {
//		for _, d := range records.Records {
//			messages = append(messages, d.Data)
//		}
//	}
//
//	return messages, nil
//}
