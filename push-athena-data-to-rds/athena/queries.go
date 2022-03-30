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

func AssetypeDataQuery(queryParams map[string]string, db string, table string) (string, types.ErrorMessage) {
	// query := ""

	stringDate := getDateString(queryParams)

	fromQuery := fmt.Sprint(db, ".", table)

	requestCaseQuery := sq.Case().
		When("split_part(clientrequesturi, '/', 2) = 'pdf'", "split_part(clientrequesturi, '/', 3)").
		Else("split_part(clientrequesturi, '/', 2)")
	
	quintypeAceCaseQuery := sq.Case().
		When("name = 'quintype-ace'", "concat('ahead_referer:', referer)").
		Else("name")

	publisherCaseQuery := sq.Case().
		When("position('%' IN url) > 0", "split_part(url, '%', 1)").
		Else("url")

	whereClause := sq.And{sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]},
		sq.Eq{"edgeresponsestatus": "200"}}

	dateQuery := fmt.Sprint("'", stringDate, "' as date")

	requestSubQuery := sq.Select().
		Prefix("WITH request(url, cache_status, response_byte, referer) AS (").
		Column(requestCaseQuery).
		Columns("cachecachestatus", "edgeresponsebytes").
		Column("concat('https://', split_part(split_part(clientrequestreferer, 'https://', 2), '/', 1))").
		From(fromQuery).
		Where(whereClause).
		Suffix("),")

	publisherDataSubQuery := sq.Select().
		Prefix("publisher_data(name, cache_status, response_byte, referer) AS (").
		Column(publisherCaseQuery).
		Columns("cache_status, response_byte, referer").
		From("request").
		Suffix("),")
	
	publisherNameSubQuery := sq.Select().
		Prefix("publisher_name(name, cache_status, response_byte) AS (").
		Column(quintypeAceCaseQuery).
		Columns("cache_status", "response_byte").
		From("publisher_data").
		Suffix(")")

	requestStringQuery, requestErrMsg := generateStringQuery(requestSubQuery)
	reqErr := requestErrMsg.Err

	if reqErr != nil {
		return "", requestErrMsg
	}

	publisherDataStringQuery, publisherDataErrMsg := generateStringQuery(publisherDataSubQuery)
	pubDataErr := publisherDataErrMsg.Err

	if pubDataErr != nil {
		return "", publisherDataErrMsg
	}

	publisherNameStringQuery, publisherNameErrMsg := generateStringQuery(publisherNameSubQuery)
	pubNameErr := publisherNameErrMsg.Err

	if pubNameErr != nil {
		return "", publisherNameErrMsg
	}

	countExp := sq.Expr("count(*)")
	hitSumExp := sq.Expr("sum(case WHEN cache_status = 'hit' THEN 1 ELSE 0 end)")
	responseByteSumExp := sq.Expr("sum(response_byte)")

	query := sq.Select().
		Prefix(requestStringQuery).
		Prefix(publisherDataStringQuery).
		Prefix(publisherNameStringQuery).
		Column("name AS publisher_name").
		Column(sq.Alias(countExp, "total_requests")).
		Column(sq.Alias(responseByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(dateQuery).
		From("publisher_name").
		GroupBy("name")

	return generateStringQuery(query)
}

func FastlyHostDataQuery(queryParams map[string]string, db string, table string) (string, types.ErrorMessage) {
	// query := -- SELECT host AS publisher_host, (count(host)) AS total_requests, (sum(output_bytes)) AS total_bytes, (sum(case when (cache_status = 'HIT' OR cache_status = 'HIT-CLUSTER') then 1 else 0 end)) AS hit_count, '2022-02-03' as date FROM qt_fastly_logs.fastly_web_logs WHERE (path NOT LIKE '%/?uptime%' AND year = 2022 AND month = 02 AND day = 03) GROUP BY host

	stringDate := getDateString(queryParams)
	fromQuery := fmt.Sprint(db, ".", table)

	reqCountExp := sq.Expr("count(host)")
	resByteSumExp := sq.Expr("sum(output_bytes)")
	hitSumExp := sq.Expr("sum(case when (cache_status = 'HIT' OR cache_status = 'HIT-CLUSTER') then 1 else 0 end)")

	dateQuery := fmt.Sprint("'", stringDate, "' as date")

	whereClause := sq.And{sq.NotLike{"path": fmt.Sprint("'", "%/?uptime%", "'")},
		sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]}}

	query := sq.Select().
		Column("host AS publisher_host").
		Column(sq.Alias(reqCountExp, "total_requests")).
		Column(sq.Alias(resByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(dateQuery).
		From(fromQuery).Where(whereClause).
		GroupBy("host")

	return generateStringQuery(query)

}

func PrimaryDomainDataQuery(queryParams map[string]string, db string, table string) (string, types.ErrorMessage) {
	// query := "SELECT clientrequesthost AS publisher_host, (count(clientrequesthost)) AS total_requests, (sum(edgeresponsebytes)) AS total_bytes, (sum(case when cachecachestatus = 'hit' then 1 else 0 end)) AS hit_count, '2022-01-17' as date FROM qt_cloudflare_logs.quintype_io WHERE (clientrequesturi NOT LIKE '%/?uptime%' AND edgepathingop <> 'ban' AND workersubrequest = false AND year = 2022 AND month = 01 AND day = 17) GROUP BY clientrequesthost;"

	stringDate := getDateString(queryParams)
	fromQuery := fmt.Sprint(db, ".", table)

	reqCountExp := sq.Expr("count(clientrequesthost)")
	resByteSumExp := sq.Expr("sum(edgeresponsebytes)")
	hitSumExp := sq.Expr("sum(case when cachecachestatus = 'hit' then 1 else 0 end)")

	dateQuery := fmt.Sprint("'", stringDate, "' as date")

	whereClause := sq.And{sq.NotLike{"clientrequesturi": fmt.Sprint("'", "%/?uptime%", "'")},
		sq.NotEq{"edgepathingop": fmt.Sprint("'", "ban", "'")},
		sq.Eq{"workersubrequest": "false"},
		sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]}}

	query := sq.Select().
		Column("clientrequesthost AS publisher_host").
		Column(sq.Alias(reqCountExp, "total_requests")).
		Column(sq.Alias(resByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(dateQuery).
		From(fromQuery).Where(whereClause).
		GroupBy("clientrequesthost")

	return generateStringQuery(query)
}

func VarnishDataQuery(queryParams map[string]string, db string, table string) (string, types.ErrorMessage) {
	// query := "WITH all_request(url, count, date) AS (SELECT split_part(request_url, '/', 3) AS publisher_name, count(*) AS total_uncached_requests, '2020-01-17' AS date FROM alb.prod_quintype_varnish  WHERE year = '2020' AND month = '01' AND day = '17' AND request_url IS NOT NULL GROUP BY split_part(request_url, '/', 3)), bulk_request(url, count, date) AS (SELECT split_part(request_url, '/', 3) AS publisher_name, count(*) AS total_uncached_requests, '2020-01-17' AS date FROM alb.prod_quintype_varnish  WHERE request_url LIKE '%/bulk%' AND year = '2020' AND month = '01' AND day = '17' AND request_url IS NOT NULL  GROUP BY  split_part(request_url, '/', 3)) SELECT a.url, a.count, b.count, a.date FROM all_request AS a FULL JOIN bulk_request AS b ON a.url = b.url; "

	stringDate := getDateString(queryParams)
	fromQuery := fmt.Sprint(db, ".", table)

	dateQuery := fmt.Sprint("'", stringDate, "'")

	yearString := fmt.Sprint("'", queryParams["year"], "'")
	monthString := fmt.Sprint("'", queryParams["month"], "'")
	dayString := fmt.Sprint("'", queryParams["day"], "'")
	bulkString := fmt.Sprint("'", "%bulk%", "'")

	bulkRequestsWhereClause := sq.And{sq.Like{"request_url": bulkString},
		sq.Eq{"year": yearString},
		sq.Eq{"month": monthString},
		sq.Eq{"day": dayString}}

	allRequestsWhereClause := sq.And{sq.Eq{"year": yearString},
		sq.Eq{"month": monthString},
		sq.Eq{"day": dayString},
		sq.NotEq{"request_url": nil}}

	allRequestSubQuery := sq.Select().
		Prefix("WITH all_request(publisher_name, total_uncached_requests, date) AS (").
		Column("split_part(request_url,'/', 3)").
		Column("count(*)").
		Column(dateQuery).
		From(fromQuery).
		Where(allRequestsWhereClause).
		GroupBy("split_part(request_url, '/', 3)").Suffix("),")

	bulkRequestQuery := sq.Select().
		Prefix("bulk_request(publisher_name, bulk_uncached_requests, date) AS (").
		Column("split_part(request_url,'/', 3) AS publisher_name").
		Column("count(*) as total_uncached_requests").
		Column(dateQuery).
		From(fromQuery).
		Where(bulkRequestsWhereClause).
		GroupBy("split_part(request_url, '/', 3)").Suffix(")")

	allRequestStringQuery, allRequestErrMsg := generateStringQuery(allRequestSubQuery)
	reqErr := allRequestErrMsg.Err

	if reqErr != nil {
		return "", allRequestErrMsg
	}
	bulkRequestStringQuery, bulkRequestErrMsg := generateStringQuery(bulkRequestQuery)
	pubDataErr := bulkRequestErrMsg.Err

	if pubDataErr != nil {
		return "", bulkRequestErrMsg
	}

	query := sq.Select().
		Prefix(allRequestStringQuery).
		Prefix(bulkRequestStringQuery).
		Column("all.publisher_name AS publisher_name").
		Column("all.total_uncached_requests as total_uncached_requests").
		Column("bulk.bulk_uncached_requests as bulk_uncached_requests").
		Column("all.date as date").
		From("all_request as all").
		Join("bulk_request as bulk on all.publisher_name = bulk.publisher_name")

	return generateStringQuery(query)
}

func FrontendHaproxyDataQuery(queryParams map[string]string, db string, table string) (string, types.ErrorMessage) {
	// query := SELECT replace(SPLIT_PART(request_url, '/', 3), ':443', '') AS domain_url, count(domain_name) AS total_requests FROM "alb"."prod_haproxy" WHERE elb_status_code = '200' AND request_url NOT LIKE '%?uptime%' AND request_url NOT LIKE '%robots.txt%' AND request_url NOT LIKE '%ping%' AND month = '03' AND year = '2020' AND day = '03' GROUP BY replace(SPLIT_PART(request_url, '/', 3), ':443', '');

	stringDate := getDateString(queryParams)
	fromQuery := fmt.Sprint(db, ".", table)

	dateQuery := fmt.Sprint("'", stringDate, "' as date")

	yearString := fmt.Sprint("'", queryParams["year"], "'")
	monthString := fmt.Sprint("'", queryParams["month"], "'")
	dayString := fmt.Sprint("'", queryParams["day"], "'")

	reqCountExp := sq.Expr("count(domain_name)")

	whereClause := sq.And{sq.Eq{"elb_status_code": fmt.Sprint("'", "200", "'")},
		sq.NotLike{"request_url": fmt.Sprint("'", "%/?uptime%", "'")},
		sq.NotLike{"request_url": fmt.Sprint("'", "%robots.txt%", "'")},
		sq.NotLike{"request_url": fmt.Sprint("'", "%ping%", "'")},
		sq.Eq{"year": yearString},
		sq.Eq{"month": monthString},
		sq.Eq{"day": dayString}}

	query := sq.Select().
		Column("replace(split_part(request_url, '/', 3), ':443', '') AS domain_url").
		Column(sq.Alias(reqCountExp, "total_requests")).
		Column(dateQuery).
		From(fromQuery).
		Where(whereClause).
		GroupBy("replace(split_part(request_url, '/', 3), ':443', '')")

	return generateStringQuery(query)
}

func GumletDataQuery(queryParams map[string]string, db string, table string) (string, types.ErrorMessage) {
	// query := SELECT split_part(split_part(clientrequesturi, '/', 2), '%', 1) AS publisher_name, (count(*)) AS total_requests, (sum(responsebytes)) AS total_bytes, (sum(case WHEN cachestatus = 'Hit' THEN 1 ELSE 0 end)) AS hit_count, '2022-02-21' AS date FROM qt_gumlet_logs.gumlet_log WHERE (statuscode <> '0' AND statuscode <> '000' AND year = 2022 AND month = 02 AND day = 21) GROUP BY split_part(split_part(clientrequesturi, '/', 2), '%', 1)
	stringDate := getDateString(queryParams)
	fromQuery := fmt.Sprint(db, ".", table)

	dateQuery := fmt.Sprint("'", stringDate, "' AS date")

	reqCountExp := sq.Expr("count(*)")
	hitSumExp := sq.Expr("sum(case WHEN cachestatus = 'Hit' THEN 1 ELSE 0 end)")
	responseByteSumExp := sq.Expr("sum(responsebytes)")

	whereClause := sq.And{sq.NotEq{"statuscode": fmt.Sprint("'", "0", "'")}, 
		sq.NotEq{"statuscode": fmt.Sprint("'", "000", "'")},
		sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]}}

	query := sq.Select().
		Column("split_part(split_part(clientrequesturi, '/', 2), '%', 1) AS publisher_name").
		Column(sq.Alias(reqCountExp, "total_requests")).
		Column(sq.Alias(responseByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(dateQuery).
		From(fromQuery).
		Where(whereClause).
		GroupBy("split_part(split_part(clientrequesturi, '/', 2), '%', 1)")

	return generateStringQuery(query)
}
