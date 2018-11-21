package main

import (
	"context"
	"encoding/json"
	"fmt"
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

func convertToAppOpticsJSON(aggregation domain.Aggregation) ([]byte, error) {
	events := []interface{}{}
	for key, value := range aggregation {
		events = append(events, map[string]interface{}{
			"name":       "platform.sketches-internal.bytes-requests",
			"time":       key.Minute,
			"attributes": map[string]bool{"aggregate": true},
			"period":     60,
			"sum":        value.TotalBytes,
			"count":      value.Count,
			"tags": map[string]string{
				"alb-name": key.AlbName,
				"host":     key.Host,
			},
		})
	}

	fullRequest := map[string]interface{}{"measurements": events}

	return json.Marshal(fullRequest)
}

func pushValuesToAppOptics(bucketName, path string) error {
	s3stream, err := domain.GetAlbLogStream(newS3Client(), bucketName, path)

	if err != nil {
		return err
	}

	logEntriesStream := domain.ParseLogFile(s3stream)

	aggregation := domain.AggregateLogEntries(logEntriesStream, strings.Split(os.Getenv("IMPORTANT_DOMAINS"), ","))

	json, err := convertToAppOpticsJSON(aggregation)

	if err != nil {
		return err
	}

	fmt.Printf("%s", json)

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
		err := pushValuesToAppOptics(os.Args[1], os.Args[2])
		if err != nil {
			fmt.Println("Failure")
			fmt.Println(err.Error())
		}
	}
}
