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

func getAssettypeDataFromAthena(athenaDBName string, athenaTableName string, s3Location string, queryParams map[string]string) {
	completeS3Location := utils.CompleteS3Location(s3Location, "assettype", queryParams)

	query, queryErrMsg := athena.AssetypeDataQuery(queryParams, athenaDBName, athenaTableName)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, queryParams, "assettype")
}

func getStatsOnPrimaryDomainFromAthena(athenaDBName string, athenaTableName string, s3Location string, queryParams map[string]string) {
	completeS3Location := utils.CompleteS3Location(s3Location, "host", queryParams)

	query, queryErrMsg := athena.PrimaryDomainDataQuery(queryParams, athenaDBName, athenaTableName)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, queryParams, "host")
}

func getVarnishDataFromAthena(athenaDBName string, athenaTableName string, s3Location string, queryParams map[string]string) {
	completeS3Location := utils.CompleteS3Location(s3Location, "varnish", queryParams)

	query, queryErrMsg := athena.VarnishDataQuery(queryParams, athenaDBName, athenaTableName)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, queryParams, "varnish")
}

func getFrontendHaproxyDataFromAthena(athenaDBName string, athenaTableName string, s3Location string, queryParams map[string]string) {
	completeS3Location := utils.CompleteS3Location(s3Location, "frontend_haproxy", queryParams)

	query, queryErrMsg := athena.FrontendHaproxyDataQuery(queryParams, athenaDBName, athenaTableName)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, completeS3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, queryParams, "frontend_haproxy")
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

func checkVariablesPresence() bool {
	_, isBucketNamePresent := os.LookupEnv("BUCKET_NAME")
	_, isS3PathPresent := os.LookupEnv("S3_FILE_PATH")
	_, isBadgerHost := os.LookupEnv("APP_HOST")
	_, isBadgerAuth := os.LookupEnv("APP_AUTH")
	_, isCloudflareDB := os.LookupEnv("CLOUDFLARE_DB")
	_, isAlbDB := os.LookupEnv("ALB_DB")
	_, isAssettypeTable := os.LookupEnv("ASSETTYPE_TABLE")
	_, isPrimaryDomainTable := os.LookupEnv("PRIMARY_DOMAIN_TABLE")
	_, isVarnishTable := os.LookupEnv("VARNISH_TABLE")
	_, isHaproxyTable := os.LookupEnv("HAPROXY_TABLE")

	if isS3PathPresent && isBucketNamePresent && isBadgerHost && isBadgerAuth && isCloudflareDB && isAlbDB && isVarnishTable && isAssettypeTable && isPrimaryDomainTable && isHaproxyTable {
		return true
	}
	return false

}

func runProcesses() {
	isAllVariablesPresent := checkVariablesPresence()

	if isAllVariablesPresent {
		queryParams := getQueryParams()

		s3Location := utils.GenerateS3Location(os.Getenv("BUCKET_NAME"), os.Getenv("S3_FILE_PATH"))

		getAssettypeDataFromAthena(os.Getenv("CLOUDFLARE_DB"), os.Getenv("ASSETTYPE_TABLE"), s3Location, queryParams)
		getStatsOnPrimaryDomainFromAthena(os.Getenv("CLOUDFLARE_DB"), os.Getenv("PRIMARY_DOMAIN_TABLE"), s3Location, queryParams)
		getVarnishDataFromAthena(os.Getenv("ALB_DB"), os.Getenv("VARNISH_TABLE"), s3Location, queryParams)
		getFrontendHaproxyDataFromAthena(os.Getenv("ALB_DB"), os.Getenv("HAPROXY_TABLE"), s3Location, queryParams)
	} else {
		fmt.Println("Enter correct BUCKET_NAME, S3_FILE_PATH, APP_HOST, APP_AUTH, CLOUDFLARE_DB, VARNISH_DB, ASSETTYPE_TABLE, PRIMARY_DOMAIN_TABLE, VARNISH_TABLE, HAPROXY_TABLE")
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
