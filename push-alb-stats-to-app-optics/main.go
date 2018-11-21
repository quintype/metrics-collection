package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

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
		pushValuesToAppOptics(s3.Bucket.Name, s3.Object.Key)
	}
}

func postToAppOptics(aggregation domain.Aggregation, appOpticsToken string) error {
	fullRequest := map[string]interface{}{"measurements": aggregation.ConvertToAppOpticsEvents()}

	json, err := json.Marshal(fullRequest)

	if err != nil {
		return err
	}

	client := &http.Client{}
	request, err := http.NewRequest("POST", "https://api.appoptics.com/v1/measurements", bytes.NewReader(json))
	request.SetBasicAuth(appOpticsToken, "")
	request.Header.Set("Content-Type", "application/json")
	result, err := client.Do(request)

	if err != nil {
		return err
	}

	defer result.Body.Close()

	io.Copy(os.Stdout, result.Body)

	if result.StatusCode != 202 {
		return fmt.Errorf("Got status %d from AppOptics", result.StatusCode)
	}

	return nil
}

func pushValuesToAppOptics(bucketName, path string) error {
	importantDomains := os.Getenv("IMPORTANT_DOMAINS")
	appOpticsToken := os.Getenv("APP_OPTICS_TOKEN")

	s3stream, err := domain.GetAlbLogStream(newS3Client(), bucketName, path)

	if err != nil {
		return err
	}

	logEntriesStream := domain.ParseLogFile(s3stream)

	aggregation := domain.AggregateLogEntries(logEntriesStream, strings.Split(importantDomains, ","))

	return postToAppOptics(aggregation, appOpticsToken)
}

func main() {
	_, isLamda := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if isLamda {
		lambda.Start(lambdaHandler)
	} else {
		if len(os.Args) != 3 {
			panic(fmt.Sprintf("Usage: %s bucket-name path-name", os.Args[0]))
		}
		err := pushValuesToAppOptics(os.Args[1], os.Args[2])
		if err != nil {
			fmt.Println("Failure")
			fmt.Println(err.Error())
		}
	}
}
