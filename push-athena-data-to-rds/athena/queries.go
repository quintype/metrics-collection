package athena

import (
	"fmt"
	"push-athena-data-to-rds/types"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

func getDateString(params map[string]string) string {
	date := []string{params["year"], params["month"], params["day"]}

	strDate := strings.Join(date, "-")

	return strDate
}

func generateStringQuery(query sq.SelectBuilder) (string, types.ErrorMessage) {
	var errMsg types.ErrorMessage
	stringQuery, args, err := query.PlaceholderFormat(sq.Dollar).ToSql()

	if err != nil {
		errMsg := types.ErrorMessage{
			Message: "Error forming the query",
			Err:     err,
		}
		return "", errMsg
	}

	for i := 1; i <= len(args); i++ {

		placeHolderString := fmt.Sprint("$", i)
		index := i - 1
		newString := args[index].(string)

		tempQuery := strings.Replace(stringQuery, placeHolderString, newString, 1)
		stringQuery = tempQuery
	}
	return stringQuery, errMsg
}

func AssetypeDataQuery(queryParams map[string]string) (string, types.ErrorMessage) {
	// query := "WITH request(url, cache_status, response_byte) AS (SELECT CASE WHEN split_part(clientrequesturi, '/', 2 ) = 'pdf' THEN split_part (clientrequesturi, '/', 3 ) ELSE split_part(clientrequesturi, '/', 2 ) END, cachecachestatus, edgeresponsebytes FROM qt_cloudflare_logs.assettype_com WHERE month = 12 AND year = 2018 AND day = 17), publisher_data(name, cache_status, response_byte) AS (SELECT CASE WHEN position('%' IN url) > 0 THEN split_part(url, '%', 1) ELSE url END, cache_status, response_byte FROM request) SELECT name, count(*) AS total_requests, sum(response_byte) AS total_bytes, sum(case WHEN cache_status = 'hit' THEN 1 ELSE 0 end) AS hit_count, '2018-12-17' AS date FROM publisher_data GROUP BY  name;"

	stringDate := getDateString(queryParams)

	requestCaseQuery := sq.Case().
		When("split_part(clientrequesturi, '/', 2) = 'pdf'", "split_part(clientrequesturi, '/', 3)").
		Else("split_part(clientrequesturi, '/', 2)")

	publisherCaseQuery := sq.Case().
		When("position('%' IN url) > 0", "split_part(url, '%', 1)").
		Else("url")

	whereClause := sq.And{sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]},
		sq.Eq{"edgeresponsestatus": "200"}}

	dateQuery := fmt.Sprint(stringDate, " as date")

	requestSubQuery := sq.Select().
		Prefix("WITH request(url, cache_status, response_byte) AS (").
		Column(requestCaseQuery).
		Columns("cachecachestatus", "edgeresponsebytes").
		From("qt_cloudflare_logs.assettype_com ").
		Where(whereClause).
		Suffix("),")

	publisherDataSubQuery := sq.Select().
		Prefix("publisher_data(name, cache_status, response_byte) AS (").
		Column(publisherCaseQuery).
		Columns("cache_status, response_byte").
		From("request").
		Suffix(")")

	requestStringQuery, requestErrMsg := generateStringQuery(requestSubQuery)
	reqErr := &requestErrMsg.Err

	if reqErr != nil {
		return "", requestErrMsg
	}
	publisherDataStringQuery, publisherDataErrMsg := generateStringQuery(publisherDataSubQuery)
	pubDataErr := &publisherDataErrMsg.Err

	if pubDataErr != nil {
		return "", publisherDataErrMsg
	}

	countExp := sq.Expr("count(*)")
	hitSumExp := sq.Expr("sum(case WHEN cache_status = 'hit' THEN 1 ELSE 0 end)")
	responseByteSumExp := sq.Expr("sum(response_byte)")

	query := sq.Select("name").
		Prefix(requestStringQuery).
		Prefix(publisherDataStringQuery).
		Column(sq.Alias(countExp, "total_requests")).
		Column(sq.Alias(responseByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(dateQuery).
		From("publisher_data").
		GroupBy("name")

	return generateStringQuery(query)
}

func QuintypeIODataQuery(queryParams map[string]string) (string, types.ErrorMessage) {
	// query := "select clientrequesthost AS publisher_name, count(clientrequesthost) as total_requests, sum(edgeresponsebytes) as total_bytes, sum(case when cachecachestatus = 'hit' then 1 else 0 end) as hit_count, '2018-12-17' AS date FROM qt_cloudflare_logs.quintype_io WHERE clientrequesturi NOT LIKE '%/?uptime%' AND clientrequesturi NOT LIKE '%ping%' AND month = 12 AND year = 2018 AND day = 17 GROUP BY  clientrequesthost;"

	stringDate := getDateString(queryParams)

	reqCountExp := sq.Expr("count(clientrequesthost)")
	resByteSumExp := sq.Expr("sum(edgeresponsebytes)")
	hitSumExp := sq.Expr("sum(case when cachecachestatus = 'hit' then 1 else 0 end)")
	dateExp := sq.Expr(stringDate)

	whereClause := sq.And{sq.NotLike{"clientrequesturi": fmt.Sprint("'", "%/?uptime%", "'")},
		sq.NotLike{"clientrequesturi": fmt.Sprint("'", "%ping%", "'")},
		sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]}}

	query := sq.Select().
		Column("clientrequesthost AS publisher_name").
		Column(sq.Alias(reqCountExp, "total_requests")).
		Column(sq.Alias(resByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(sq.Alias(dateExp, "date")).
		From("qt_cloudflare_logs.quintype_io").Where(whereClause).
		GroupBy("clientrequesthost")

	return generateStringQuery(query)
}

func VarnishDataQuery(queryParams map[string]string) (string, types.ErrorMessage) {
	// query := "SELECT split_part(request_url,'/', 3) AS publisher_name, count(*) AS total_uncached_requests, '2018-12-17' AS date FROM alb.prod_qtproxy_varnish_internal WHERE year = '2018' AND month = '12' AND day = '17' and request_url IS NOT NULL GROUP BY split_part(request_url, '/', 3);"

	stringDate := getDateString(queryParams)

	dateQuery := fmt.Sprint(stringDate, " AS date")

	yearString := fmt.Sprint("'", queryParams["year"], "'")
	monthString := fmt.Sprint("'", queryParams["month"], "'")
	dayString := fmt.Sprint("'", queryParams["day"], "'")

	whereClause := sq.And{sq.Eq{"year": yearString},
		sq.Eq{"month": monthString},
		sq.Eq{"day": dayString},
		sq.NotEq{"request_url": nil}}

	query := sq.Select().
		Column("split_part(request_url,'/', 3) AS publisher_name").
		Column("count(*) as total_uncached_requests").
		Column(dateQuery).
		From("alb.prod_qtproxy_varnish_internal").
		Where(whereClause).
		GroupBy("split_part(request_url, '/', 3)")

	return generateStringQuery(query)
}
