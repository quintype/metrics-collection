package main

import (
	"fmt"
	"os"

	"push-alb-stats-to-app-optics/domain"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func newS3Client() *s3.S3 {
	return s3.New(session.Must(session.NewSession()))
}

func lambdaHandler() (string, error) {
	return "Hello ƛ!", nil
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
