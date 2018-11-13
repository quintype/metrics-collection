package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"push-alb-stats-to-app-optics/domain"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func newS3Client() *s3.S3 {
	return s3.New(session.Must(session.NewSession()))
}

func lambdaHandler(ctx context.Context, s3Event events.S3Event) {
	for _, record := range s3Event.Records {
		s3 := record.S3
		log.Printf("[%s - %s] Bucket = %s, Key = %s \n", record.EventSource, record.EventTime, s3.Bucket.Name, s3.Object.Key)
	}
}

func localHandler(bucketName, path string) error {
	s3stream, err := domain.GetAlbLogStream(newS3Client(), bucketName, path)

	if err != nil {
		return err
	}

	for item := range domain.ParseLogFile(s3stream) {
		fmt.Printf("%v\n", item)
	}

	return nil
}

func main() {
	_, isLamda := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if isLamda {
		lambda.Start(lambdaHandler)
	} else {
		if len(os.Args) != 3 {
			panic(fmt.Sprintf("Usage: %s bucket-name path-name", os.Args[0]))
		}
		err := localHandler(os.Args[1], os.Args[2])
		if err != nil {
			fmt.Println("Failure")
			fmt.Println(err.Error())
		}
	}
}
