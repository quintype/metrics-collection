package athena

import (
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

func getDateString(params map[string]string) string {
	date := []string{params["year"], params["month"], params["day"]}

	strDate := strings.Join(date, "-")

	return strDate
}

func generateQueryWithDate(query string, queryParams []interface{}) string {
	for i := 1; i <= len(queryParams); i++ {

		placeHolderString := fmt.Sprint("$", i)
		index := i - 1
		newString := queryParams[index].(string)

		tempQuery := strings.Replace(query, placeHolderString, newString, 1)
		query = tempQuery
	}
	return query
}

func AssetypeQuery(queryParams map[string]string) string {
	stringDate := getDateString(queryParams)

	requestCaseQuery := sq.Case().
		When("split_part(clientrequesturi, '/', 2) = 'pdf'", "split_part(clientrequesturi, '/', 3)").
		Else("split_part(clientrequesturi, '/', 2)")

	publisherCaseQuery := sq.Case().
		When("position('%' IN url) > 0", "split_part(url, '%', 1)").
		Else("url")

	whereClause := sq.And{sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]}}

	dateQuery := fmt.Sprint(stringDate, " as date")

	requestSubQuery, args, _ := sq.Select().
		Prefix("WITH request(url, cache_status, response_byte) AS (").
		Column(requestCaseQuery).
		Columns("cachecachestatus", "edgeresponsebytes").
		From("qt_cloudflare_logs.assettype_com ").
		Where(whereClause).
		Suffix("),").
		PlaceholderFormat(sq.Dollar).
		ToSql()

	publisherDataSubQuery, _, _ := sq.Select().
		Prefix("publisher_data(name, cache_status, response_byte) AS (").
		Column(publisherCaseQuery).
		Columns("cache_status, response_byte").
		From("request").
		Suffix(")").
		ToSql()

	countExp := sq.Expr("count(*)")
	hitSumExp := sq.Expr("sum(case WHEN cache_status = 'hit' THEN 1 ELSE 0 end)")
	responseByteSumExp := sq.Expr("sum(response_byte)")

	query, _, _ := sq.Select("name").
		Prefix(requestSubQuery).
		Prefix(publisherDataSubQuery).
		Column(sq.Alias(countExp, "total_requests")).
		Column(sq.Alias(responseByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(dateQuery).
		From("publisher_data").
		GroupBy("name").
		ToSql()

	return generateQueryWithDate(query, args)
}

func HostQuery(queryParams map[string]string) string {
	stringDate := getDateString(queryParams)

	caseQuery := sq.Case().
		When("split_part(clientrequesthost, '.', 1) = 'www'", "split_part(clientrequesthost, '.', 2)").
		When("split_part(clientrequesthost, '.', 1) = 'beta'", "split_part(clientrequesthost, '.', 2)").
		When("split_part(clientrequesthost, '.', 1) = 'fit' OR split_part(clientrequesthost, '.', 1) = 'hindi'", "concat(split_part(clientrequesthost, '.', 1), '.', split_part(clientrequesthost, '.', 2))").
		Else("split_part(clientrequesthost, '.', 1)")

	reqCountExp := sq.Expr("count(clientrequesthost)")
	resByteSumExp := sq.Expr("sum(edgeresponsebytes)")
	hitSumExp := sq.Expr("sum(case when cachecachestatus = 'hit' then 1 else 0 end)")
	dateExp := sq.Expr(stringDate)

	whereClause := sq.And{sq.NotLike{"clientrequesturi": fmt.Sprint("'", "%/?uptime%", "'")},
		sq.NotLike{"clientrequesturi": fmt.Sprint("'", "%ping%", "'")},
		sq.Eq{"year": queryParams["year"]},
		sq.Eq{"month": queryParams["month"]},
		sq.Eq{"day": queryParams["day"]}}

	query, args, _ := sq.Select().
		Column(sq.Alias(caseQuery, "publisher_name")).
		Column(sq.Alias(reqCountExp, "total_requests")).
		Column(sq.Alias(resByteSumExp, "total_bytes")).
		Column(sq.Alias(hitSumExp, "hit_count")).
		Column(sq.Alias(dateExp, "date")).
		From("qt_cloudflare_logs.quintype_io").Where(whereClause).
		GroupBy("clientrequesthost").
		PlaceholderFormat(sq.Dollar).
		ToSql()

	return generateQueryWithDate(query, args)
}

func UncachedQuery(queryParams map[string]string) string {
	stringDate := getDateString(queryParams)

	dateQuery := fmt.Sprint(stringDate, " AS date")

	yearString := fmt.Sprint("'", queryParams["year"], "'")
	monthString := fmt.Sprint("'", queryParams["month"], "'")
	dayString := fmt.Sprint("'", queryParams["day"], "'")

	whereClause := sq.And{sq.Eq{"year": yearString}, sq.Eq{"month": monthString}, sq.Eq{"day": dayString}, sq.NotEq{"request_url": nil}}

	query, args, _ := sq.Select().
		Column("split_part(split_part(request_url,'/', 3), '.', 1) AS publisher_name").
		Column("count(*) as total_uncached_requests").
		Column(dateQuery).
		From("alb.prod_qtproxy_varnish_internal").
		Where(whereClause).
		GroupBy("split_part(split_part(request_url, '/', 3), '.', 1)").
		PlaceholderFormat(sq.Dollar).
		ToSql()

	return generateQueryWithDate(query, args)
}
