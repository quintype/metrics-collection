package main

import (
	"context"
	"fmt"
	"os"
	"push-athena-data-to-rds/api"
	"push-athena-data-to-rds/athena"
	"push-athena-data-to-rds/utils"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

func getAssettypeDataFromAthena(athenaDBName string, s3Location string, queryParams map[string]string) {
	completeS3Location := fmt.Sprint(s3Location, "/assettype")

	query, queryErrMsg := athena.AssetypeDataQuery(queryParams)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, "assettype")
}

func getQuintypeIODataFromAthena(athenaDBName string, s3Location string, queryParams map[string]string) {
	completeS3Location := fmt.Sprint(s3Location, "/host")

	query, queryErrMsg := athena.QuintypeIODataQuery(queryParams)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, "host")
}

func getVarnishDataFromAthena(athenaDBName string, s3Location string, queryParams map[string]string) {
	completeS3Location := fmt.Sprint(s3Location, "/varnish")

	query, queryErrMsg := athena.VarnishDataQuery(queryParams)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, "varnish")
}

func getQueryParams() map[string]string {
	var queryParams map[string]string
	_, isDatePresent := os.LookupEnv("DATE")

	if isDatePresent {
		inputDate := os.Getenv("DATE")
		isValidDate := utils.ValidateDate(inputDate)

		if isValidDate {
			splitDate := strings.Split(inputDate, "-")

			queryParams = map[string]string{
				"year":  splitDate[0],
				"month": splitDate[1],
				"day":   splitDate[2],
			}
		} else {
			fmt.Println("Invalid Date Entered")
		}
	} else {
		dateYear, dateMonth, dateDay := time.Now().AddDate(0, 0, -1).Date()
		monthNumber := int(dateMonth)
		var strMonth, strDay string

		if monthNumber >= 1 && monthNumber <= 9 {
			strMonth = fmt.Sprint("0", strconv.Itoa(monthNumber))
		} else {
			strMonth = strconv.Itoa(monthNumber)
		}

		if dateDay >= 1 && dateDay <= 9 {
			strDay = fmt.Sprint("0", strconv.Itoa(dateDay))
		} else {
			strDay = strconv.Itoa(dateDay)
		}

		queryParams = map[string]string{
			"year":  strconv.Itoa(dateYear),
			"month": strMonth,
			"day":   strDay,
		}
	}
	return queryParams
}

func runProcesses() {
	_, isBucketNamePresent := os.LookupEnv("BUCKET_NAME")
	_, isS3PathPresent := os.LookupEnv("S3_FILE_PATH")
	_, isBadgerHost := os.LookupEnv("APP_HOST")

	if isS3PathPresent && isBucketNamePresent && isBadgerHost {
		queryParams := getQueryParams()

		s3Location := utils.GenerateS3Location(os.Getenv("BUCKET_NAME"), os.Getenv("S3_FILE_PATH"))

		getAssettypeDataFromAthena("qt_cloudflare_logs", s3Location, queryParams)
		getQuintypeIODataFromAthena("qt_cloudflare_logs", s3Location, queryParams)
		getVarnishDataFromAthena("alb", s3Location, queryParams)
	} else {
		fmt.Println("Enter correct BUCKET_NAME, S3_FILE_PATH and APP_HOST")
	}
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
