package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/dustin/go-humanize"
	"go_kinesis/models"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"

	"github.com/joho/godotenv"
)

var (
	awsConfig models.AwsConfig
	_         = fmt.Print
	count     int
	batch     int
)

// initiate configuration
func init() {
	e := godotenv.Load() //Load .env file
	if e != nil {
		fmt.Print(e)
	}
	awsConfig = models.AwsConfig{
		Stream:          os.Getenv("KINESIS_STREAM_NAME"),
		Region:          os.Getenv("KINESIS_REGION"),
		Endpoint:        os.Getenv("AWS_ENDPOINT"),
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
	}

	flag.IntVar(&count, "count", 10000, "Number of documents to generate")
	flag.IntVar(&batch, "batch", 255, "Number of documents to send in one batch")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())
}

func main() {
	start := time.Now().UTC()
	var (
		//buf        bytes.Buffer
		students   []*models.Student
		numItems   int
		numErrors  int
		numIndexed int
		numBatches int
		currBatch  int
	)
	names := []string{"Alice", "John", "Mary"}
	for i := 1; i < count+1; i++ {
		students = append(students, &models.Student{
			Id:           int64(i),
			Name:         strings.Join([]string{names[rand.Intn(len(names))], strconv.Itoa(i)}, " "),
			Age:          int64(rand.Intn(10) + 12),
			AverageScore: float64(rand.Intn(30) + 50),
		})
	}
	log.Printf("→ Generated %s students", humanize.Comma(int64(len(students))))

	// setup kinesis client.
	s := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(awsConfig.Region),
		Endpoint:    aws.String(awsConfig.Endpoint),
		Credentials: credentials.NewStaticCredentials(awsConfig.AccessKeyID, awsConfig.SecretAccessKey, awsConfig.SessionToken),
	}))
	kc := kinesis.New(s)
	streamName := aws.String(awsConfig.Stream)
	_, err := kc.DescribeStream(&kinesis.DescribeStreamInput{StreamName: streamName})

	//if no streamer name in AWS
	if err != nil {
		log.Panic(err)
	}

	numBatches = len(students)%batch + 1
	log.Printf("We have %v batches need to be put", numBatches)

	for i, a := range students {
		numItems++

		currBatch = i / batch
		if i == count-1 {
			currBatch++
		}

		data, err := json.Marshal(a)
		if err != nil {
			log.Fatalf("Cannot encode article %d: %s", a.Id, err)
		}

		putOutput, err := kc.PutRecord(&kinesis.PutRecordInput{
			Data:         data,
			StreamName:   streamName,
			PartitionKey: aws.String("key1"),
		})
		if err != nil {
			panic(err)
		}
		numIndexed++
		fmt.Printf("%v\n", *putOutput)
		time.Sleep(50 * time.Millisecond)
	}

	dur := time.Since(start)

	if numErrors > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			humanize.Comma(int64(numErrors)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	} else {
		log.Printf(
			"Sucessfuly sent [%s] batches in %s (%s docs/sec)",
			humanize.Comma(int64(numIndexed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(numIndexed))),
		)
	}
}
