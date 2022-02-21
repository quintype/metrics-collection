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

func getSourceDetails(dataSource string, queryParams map[string]string) (string, string, types.ErrorMessage) {
	switch dataSource {
	case "assettype":
		{
			athenaDBName := os.Getenv("CLOUDFLARE_DB")
			athenaTableName := os.Getenv("ASSETTYPE_TABLE")
			query, queryErrMsg := athena.AssetypeDataQuery(queryParams, athenaDBName, athenaTableName)
			return athenaDBName, query, queryErrMsg
		}
	case "host":
		{
			athenaDBName := os.Getenv("CLOUDFLARE_DB")
			athenaTableName := os.Getenv("PRIMARY_DOMAIN_TABLE")
			query, queryErrMsg := athena.PrimaryDomainDataQuery(queryParams, athenaDBName, athenaTableName)
			return athenaDBName, query, queryErrMsg
		}
	case "fastly_host":
		{
			athenaDBName := os.Getenv("FASTLY_DB")
			athenaTableName := os.Getenv("FASTLY_PRIMARY_DOMAIN_TABLE")
			query, queryErrMsg := athena.FastlyHostDataQuery(queryParams, athenaDBName, athenaTableName)
			return athenaDBName, query, queryErrMsg
		}
	case "varnish":
		{
			athenaDBName := os.Getenv("ALB_DB")
			athenaTableName := os.Getenv("VARNISH_TABLE")
			query, queryErrMsg := athena.VarnishDataQuery(queryParams, athenaDBName, athenaTableName)
			return athenaDBName, query, queryErrMsg
		}
	case "frontend_haproxy":
		{
			athenaDBName := os.Getenv("ALB_DB")
			athenaTableName := os.Getenv("HAPROXY_TABLE")
			query, queryErrMsg := athena.FrontendHaproxyDataQuery(queryParams, athenaDBName, athenaTableName)
			return athenaDBName, query, queryErrMsg
		}
	case "gumlet":
		{
			athenaDBName := os.Getenv("GUMLET_DB")
			athenaTableName := os.Getenv("GUMLET_TABLE")
			query, queryErrMsg := athena.GumletDataQuery(queryParams, athenaDBName, athenaTableName)
			return athenaDBName, query, queryErrMsg
		}
	default:
		{
			query, queryErrMsg := "", types.ErrorMessage{
				Message: "Wrong DataSource",
				Err:     nil,
			}
			return "Wrong DataSource", query, queryErrMsg
		}
	}
}

func getDataFromAthena(dataSource string, s3Location string, queryParams map[string]string) {
	athenaDBName, query, queryErrMsg := getSourceDetails(dataSource, queryParams)

	if len(query) == 0 {
		fmt.Println(queryErrMsg.Message, dataSource)
		return
	}

	s3FileName, queryStatus, athenaErrMsg := athena.SaveDataToS3(query, athenaDBName, s3Location)

	if athenaErrMsg.Err != nil {
		fmt.Println(athenaErrMsg.Message, athenaErrMsg.Err)
		return
	}

	if queryStatus == "SUCCEEDED" {
		api.SaveAthenaData(s3FileName, queryParams, dataSource)
		return
	}

	fmt.Println("Athena query execution was", queryStatus)
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

func getMissingVariables() []string {
	envVariables := []string{"BUCKET_NAME", "S3_FILE_PATH", "APP_HOST", "APP_AUTH", "CLOUDFLARE_DB", "ALB_DB", "ASSETTYPE_TABLE", "PRIMARY_DOMAIN_TABLE", "VARNISH_TABLE", "HAPROXY_TABLE", "GUMLET_DB", "GUMLET_TABLE", "FASTLY_DB", "FASTLY_PRIMARY_DOMAIN_TABLE"}

	var missingVariables []string

	for i := 0; i < len(envVariables); i++ {
		envValue, _ := os.LookupEnv(envVariables[i])

		if len(envValue) <= 0 {
			missingVariables = append(missingVariables, envVariables[i])
		}
	}

	return missingVariables
}

func runProcesses() {
	missingVariables := getMissingVariables()

	if len(missingVariables) <= 0 {
		queryParams := getQueryParams()

		dataSources := []string{"assettype", "host", "fastly_host", "varnish", "frontend_haproxy", "gumlet"}

		for index := 0; index < len(dataSources); index++ {
			dataSource := dataSources[index]

			s3Location := utils.GenerateS3Location(os.Getenv("BUCKET_NAME"), os.Getenv("S3_FILE_PATH"), dataSource, queryParams)

			getDataFromAthena(dataSource, s3Location, queryParams)
		}
	} else {
		fmt.Println("Enter value for missing variables", missingVariables)
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
