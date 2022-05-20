package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"sync"
	"time"
)

func generateData(numberOfIds int, intervalInMs int, ch chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < numberOfIds; i++ {
		id := uuid.NewString()
		fmt.Println()
		fmt.Println("##### STARTED PROCESSING MESSAGE")
		fmt.Println("Inserting", id, "into channel")
		ch <- id
		time.Sleep(time.Millisecond * time.Duration(intervalInMs))
	}
}

func readData(client *kinesis.Client, stream string, partitionKey string, ch <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		data := <-ch
		fmt.Println("Channel received a new message:", data)
		streamData(client, stream, partitionKey, data)
	}
}

func streamData(client *kinesis.Client, stream string, partitionKey string, data string) {
	putRecordInput := kinesis.PutRecordInput{
		Data:         []byte(data),
		PartitionKey: &partitionKey,
		StreamName:   &stream,
	}
	fmt.Println("Streaming", data, "to the partition key", partitionKey, "from the", stream, "stream")
	putRecordOutput, _ := client.PutRecord(context.TODO(), &putRecordInput)
	fmt.Println(data, "streamed to shard", *putRecordOutput.ShardId, "with SequenceNumber", *putRecordOutput.SequenceNumber)
	fmt.Println("##### FINISHED PROCESSING MESSAGE")
}

func main() {
	fmt.Println("Running Kinesis Producer Example...")

	// Parameters
	goroutines := 2

	awsSharedConfigProfile := "personal-account"
	awsRegion := "us-east-1"

	numberOfIds := 5
	intervalInMs := 2000

	stream := "KinesisLearning"
	partitionKey := "agenda-builder"

	// Setting up WaitGroup
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Setting up Kinesis Client
	cfg, _ := config.LoadDefaultConfig(
		context.TODO(),
		config.WithSharedConfigProfile(awsSharedConfigProfile),
		config.WithRegion(awsRegion),
	)
	client := kinesis.NewFromConfig(cfg)

	// Starting Goroutines
	ch := make(chan string)
	go generateData(numberOfIds, intervalInMs, ch, &wg)
	go readData(client, stream, partitionKey, ch, &wg)

	wg.Wait()
}
