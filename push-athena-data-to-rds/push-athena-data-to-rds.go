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

func getAssettypeData(queryParams map[string]string) {
	dbName := "qt_cloudflare_logs"
	s3Location := "s3://aws-athena-query-results-687145066723-us-east-1/boto3/cloudflare/billing-test-data/assettype"

	// query := "WITH request(url, cache_status, response_byte) AS (SELECT CASE WHEN split_part(clientrequesturi, '/', 2 ) = 'pdf' THEN split_part (clientrequesturi, '/', 3 ) ELSE split_part(clientrequesturi, '/', 2 ) END, cachecachestatus, edgeresponsebytes FROM qt_cloudflare_logs.assettype_com WHERE month = 12 AND year = 2018 AND day = 17), publisher_data(name, cache_status, response_byte) AS (SELECT CASE WHEN position('%' IN url) > 0 THEN split_part(url, '%', 1) ELSE url END, cache_status, response_byte FROM request) SELECT name, count(*) AS total_requests, sum(response_byte) AS total_bytes, sum(case WHEN cache_status = 'hit' THEN 1 ELSE 0 end) AS hit_count, '2018-12-17' AS date FROM publisher_data GROUP BY  name;"

	query, _ := athena.AssetypeDataQuery(queryParams)

	athena.SaveDataToS3(query, dbName, s3Location)
}

func getHostData(queryParams map[string]string) {
	dbName := "qt_cloudflare_logs"
	s3Location := "s3://aws-athena-query-results-687145066723-us-east-1/boto3/cloudflare/billing-test-data/host"

	// query := "select CASE WHEN split_part(clientrequesthost, '.', 1) = 'www' THEN split_part(clientrequesthost, '.', 2) WHEN split_part(clientrequesthost, '.', 1) = 'beta' THEN split_part(clientrequesthost, '.', 2) WHEN split_part(clientrequesthost, '.', 1) = 'fit' OR split_part(clientrequesthost, '.', 1) = 'hindi' THEN concat(split_part(clientrequesthost, '.', 1), '.', split_part(clientrequesthost, '.', 2)) WHEN split_part(clientrequesthost, '.', 1) = 'hindi' THEN split_part(clientrequesthost, '.', 2) ELSE split_part(clientrequesthost, '.', 1) END AS publisher_name, count(clientrequesthost) as total_requests, sum(edgeresponsebytes) as total_bytes, sum(case when cachecachestatus = 'hit' then 1 else 0 end) as hit_count, '2018-12-17' AS date FROM qt_cloudflare_logs.quintype_io WHERE clientrequesturi NOT LIKE '%/?uptime%' AND clientrequesturi NOT LIKE '%ping%' AND month = 12 AND year = 2018 AND day = 17 GROUP BY  clientrequesthost;"

	query, _ := athena.QuintypeIODataQuery(queryParams)

	s3FileName, errMessage := athena.SaveDataToS3(query, dbName, s3Location)

	fmt.Println(s3FileName, errMessage)
}

func getUncachedData(queryParams map[string]string) {
	dbName := "alb"
	s3Location := "s3://aws-athena-query-results-687145066723-us-east-1/boto3/cloudflare/billing-test-data/uncached"

	// query := "SELECT split_part(split_part(request_url,'/', 3), '.', 1) AS publisher_name, count(*) AS total_uncached_requests, '2018-12-17' AS date FROM alb.prod_qtproxy_varnish_internal WHERE year = '2018' AND month = '12' AND day = '17' and request_url IS NOT NULL GROUP BY split_part(split_part(request_url, '/', 3), '.', 1);"

	query, _ := athena.VarnishDataQuery(queryParams)

	athena.SaveDataToS3(query, dbName, s3Location)
}

func runProcesses() {
	var queryParams map[string]string

	_, isDatePresent := os.LookupEnv("DATE")

	if isDatePresent && os.Getenv("DATE") == "" {
		dateYear, dateMonth, dateDay := time.Now().Date()
		monthNumber := int(dateMonth)

		queryParams["year"] = strconv.Itoa(dateYear)
		queryParams["month"] = strconv.Itoa(monthNumber)
		queryParams["day"] = strconv.Itoa(dateDay)

	} else {
		date := os.Getenv("DATE")
		splitDate := strings.Split(date, "-")

		queryParams["year"] = splitDate[0]
		queryParams["month"] = splitDate[1]
		queryParams["day"] = splitDate[2]
	}

	getAssettypeData(queryParams)
	getHostData(queryParams)
	getUncachedData(queryParams)
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
