package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func newSQSClient() *sqs.Client {

	// Create an SQS client
	sqsConfig, err := config.LoadDefaultConfig(context.TODO())
	fatalIfError(err)
	return sqs.NewFromConfig(sqsConfig)
}

func queueUrl(client *sqs.Client, queueName string) *string {
	// get the queue URL from the name
	result, err := client.GetQueueUrl(context.TODO(), &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	fatalIfError(err)
	return result.QueueUrl
}

func getSingleMessage(client *sqs.Client, queueUrl *string, timeout int32) (*types.Message, error) {
	result, err := client.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            queueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     timeout,
	})
	if err != nil {
		return nil, err
	}
	if len(result.Messages) == 1 {
		return &result.Messages[0], nil
	}
	return nil, nil
}

func heartbeatMessage(client *sqs.Client, queueUrl *string, handle *string, timeout int32) error {

	_, err := client.ChangeMessageVisibility(context.TODO(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          queueUrl,
		ReceiptHandle:     handle,
		VisibilityTimeout: timeout + 30, // reset it to 30 seconds larger than the heartbeat timeout
	})
	return err
}

func deleteMessage(client *sqs.Client, queueUrl *string, handle *string) error {

	_, err := client.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      queueUrl,
		ReceiptHandle: handle,
	})
	return err
}

//
// end of file
//
