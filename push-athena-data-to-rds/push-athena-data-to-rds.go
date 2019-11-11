package main

import (
	"context"
	"fmt"
	"os"
	"push-athena-data-to-rds/api"
	"push-athena-data-to-rds/athena"
	"push-athena-data-to-rds/types"
	"push-athena-data-to-rds/utils"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
)

func getQuery(dataSource string, queryParams map[string]string, athenaDBName string, athenaTableName string) (string, types.ErrorMessage) {
	switch dataSource {
	case "assettype":
		return athena.AssetypeDataQuery(queryParams, athenaDBName, athenaTableName)
	case "host":
		return athena.PrimaryDomainDataQuery(queryParams, athenaDBName, athenaTableName)
	case "varnish":
		return athena.VarnishDataQuery(queryParams, athenaDBName, athenaTableName)
	case "frontend_haproxy":
		return athena.FrontendHaproxyDataQuery(queryParams, athenaDBName, athenaTableName)
	case "gumlet":
		return athena.GumletDataQuery(queryParams, athenaDBName, athenaTableName)
	default:
		return "", types.ErrorMessage{
			Message: "Wrong DataSource",
			Err:     nil,
		}
	}
}

func getDBName(dataSource string) string {
	switch dataSource {
	case "assettype":
		return os.Getenv("CLOUDFLARE_DB")
	case "host":
		return os.Getenv("CLOUDFLARE_DB")
	case "varnish":
		return os.Getenv("ALB_DB")
	case "frontend_haproxy":
		return os.Getenv("ALB_DB")
	case "gumlet":
		return os.Getenv("GUMLET_DB")
	default:
		return "Wrong DataSource"
	}
}

func getTableName(dataSource string) string {
	switch dataSource {
	case "assettype":
		return os.Getenv("ASSETTYPE_TABLE")
	case "host":
		return os.Getenv("PRIMARY_DOMAIN_TABLE")
	case "varnish":
		return os.Getenv("VARNISH_TABLE")
	case "frontend_haproxy":
		return os.Getenv("HAPROXY_TABLE")
	case "gumlet":
		return os.Getenv("GUMLET_TABLE")
	default:
		return "Wrong DataSource"
	}
}

func getDataFromAthena(dataSource string, s3Location string, queryParams map[string]string) {
	athenaDBName := getDBName(dataSource)
	athenaTableName := getTableName(dataSource)
	query, queryErrMsg := getQuery(dataSource, queryParams, athenaDBName, athenaTableName)

	if queryErrMsg.Err != nil {
		fmt.Println(queryErrMsg.Message, queryErrMsg.Err)
	}

	s3FileName, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, s3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
	}

	api.SaveAthenaData(s3FileName, queryParams, dataSource)
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
	_, isGumletDB := os.LookupEnv("GUMLET_DB")
	_, isGumletTable := os.LookupEnv("GUMLET_TABLE")

	if isS3PathPresent && isBucketNamePresent && isBadgerHost && isBadgerAuth && isCloudflareDB && isAlbDB && isVarnishTable && isAssettypeTable && isPrimaryDomainTable && isHaproxyTable && isGumletDB && isGumletTable {
		return true
	}
	return false

}

func runProcesses() {
	isAllVariablesPresent := checkVariablesPresence()

	if isAllVariablesPresent {
		queryParams := getQueryParams()

		dataSources := []string{"assettype", "host", "varnish", "frontend_haproxy", "gumlet"}

		for index := 0; index < len(dataSources); index++ {
			dataSource := dataSources[index]

			s3Location := utils.GenerateS3Location(os.Getenv("BUCKET_NAME"), os.Getenv("S3_FILE_PATH"), dataSource, queryParams)

			getDataFromAthena(dataSource, s3Location, queryParams)
		}
	} else {
		fmt.Println("Enter correct BUCKET_NAME, S3_FILE_PATH, APP_HOST, APP_AUTH, CLOUDFLARE_DB, VARNISH_DB, ASSETTYPE_TABLE, PRIMARY_DOMAIN_TABLE, VARNISH_TABLE, HAPROXY_TABLE, GUMLET_DB, GUMLET_TABLE ")
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
