package fetcher

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"go_kinesis_es_project/models"
	"log"
)

func Fetch(awsConfig models.AwsConfig) ([][]byte, error) {
	// connect to aws config kinesis
	s := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(awsConfig.Region),
		Endpoint:    aws.String(awsConfig.Endpoint),
		Credentials: credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, awsConfig.SessionToken),
	}))
	kc := kinesis.New(s)
	streamName := aws.String(awsConfig.Stream)
	streams, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})
	if err != nil {
		log.Panic(err)
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
		log.Panic(err)
		return nil, err
	}

	shardIterator := iteratorOutput.ShardIterator
	// get records use shard iterator for making request
	records, err := kc.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: shardIterator,
	})

	// if error, wait until 1 seconds and continue the looping process
	if err != nil {
		return nil, err
	}
	var messages [][]byte
	if len(records.Records) > 0 {
		for _, d := range records.Records {
			messages = append(messages, d.Data)
		}
	}

	return messages, nil
}
