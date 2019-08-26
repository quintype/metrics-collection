package main

import (
	"context"
	"fmt"
	"os"
	"push-athena-data-to-rds/athena"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

func getAssettypeDataFromAthena(queryParams map[string]string) string {
	dbName := "qt_cloudflare_logs"
	s3Location := "s3://aws-athena-query-results-687145066723-us-east-1/boto3/cloudflare/billing-test-data/assettype"

	query, queryErrMsg := athena.AssetypeDataQuery(queryParams)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
		return ""
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, dbName, s3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
		return ""
	}

	return s3FileName
}

func getQuintypeIODataFromAthena(queryParams map[string]string) string {
	dbName := "qt_cloudflare_logs"
	s3Location := "s3://aws-athena-query-results-687145066723-us-east-1/boto3/cloudflare/billing-test-data/host"

	query, queryErrMsg := athena.QuintypeIODataQuery(queryParams)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
		return ""
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, dbName, s3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
		return ""
	}

	return s3FileName
}

func getVarnishDataFromAthena(queryParams map[string]string) string {
	dbName := "alb"
	s3Location := "s3://aws-athena-query-results-687145066723-us-east-1/boto3/cloudflare/billing-test-data/uncached"

	query, queryErrMsg := athena.VarnishDataQuery(queryParams)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
		return ""
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, dbName, s3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
		return ""
	}

	return s3FileName
}

func getQueryParams() map[string]string {
	var queryParams map[string]string
	_, isDatePresent := os.LookupEnv("DATE")

	if !isDatePresent && os.Getenv("DATE") == "" {
		dateYear, dateMonth, dateDay := time.Now().Date()
		monthNumber := int(dateMonth)

		queryParams = map[string]string{
			"year":  strconv.Itoa(dateYear),
			"month": strconv.Itoa(monthNumber),
			"day":   strconv.Itoa(dateDay),
		}

	} else {
		date := os.Getenv("DATE")
		splitDate := strings.Split(date, "-")

		queryParams = map[string]string{
			"year":  splitDate[0],
			"month": splitDate[1],
			"day":   splitDate[2],
		}
	}
	return queryParams
}

func runProcesses() {
	queryParams := getQueryParams()

	getAssettypeDataFromAthena(queryParams)
	getQuintypeIODataFromAthena(queryParams)
	getVarnishDataFromAthena(queryParams)
}

func lambdaHandler(ctx context.Context) {
	runProcesses()
}

func main() {
	_, isLamda := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME")
	if isLamda {
		lambda.Start(lambdaHandler)
	} else {
		runProcesses()
	}
}
