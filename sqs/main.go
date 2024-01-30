package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var sess = session.Must(session.NewSessionWithOptions(session.Options{
	SharedConfigState: session.SharedConfigEnable,
}))

var sqsSvc = sqs.New(sess)

var queueURL = "https://sqs.us-east-2.amazonaws.com/174821900730/message-test-queue"

var sqsMaxMessages = aws.Int64(10)

var sqsPollWaitSeconds = aws.Int64(10)

func main() {

	messageBody := "SQS Message"

	chnSendMessages := make(chan error, 1)
	go sendMessages(messageBody, chnSendMessages)
	go handleSendMessage(chnSendMessages)

	chnMessages := make(chan *sqs.Message, 1)
	go pollSqs(chnMessages)

	for message := range chnMessages {
		handleMessage(message)
		deleteMessage(message)
	}

}

func handleMessage(msg *sqs.Message) {
	fmt.Println("Message received: ")
	fmt.Println(*msg.Body)
}

func deleteMessage(msg *sqs.Message) {
	sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
}

func pollSqs(chn chan<- *sqs.Message) {

	for {
		output, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &queueURL,
			MaxNumberOfMessages: aws.Int64(*sqsMaxMessages),
			WaitTimeSeconds:     aws.Int64(*sqsPollWaitSeconds),
		})

		if err != nil {
			fmt.Println(err.Error())
		}

		for _, message := range output.Messages {
			chn <- message
		}

	}

}

func handleSendMessage(chn chan error) {

	for err := range chn {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

}

func sendMessages(messageBody string, chn chan<- error) {

	count := 0
	for {
		time.Sleep(3 * time.Second)
		count++
		message := messageBody + " " + strconv.Itoa(count)
		_, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String(message),
		})
		chn <- err
	}

}
