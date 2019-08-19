package athena

import (
	"fmt"
	"strconv"
	"strings"

	sq "github.com/Masterminds/squirrel"
)

func getDateString(year int, month int, day int) string {
	date := []string{strconv.Itoa(year), strconv.Itoa(month), strconv.Itoa(day)}

	strDate := strings.Join(date, "-")

	return strDate
}

func AssetypeQuery(year int, month int, day int) {
	// query := WITH request(url, cache_status, response_byte) AS (SELECT CASE WHEN split_part(clientrequesturi, '/', 2 ) = 'pdf' THEN split_part (clientrequesturi, '/', 3 ) ELSE split_part(clientrequesturi, '/', 2 ) END, cachecachestatus, edgeresponsebytes FROM qt_cloudflare_logs.assettype_com WHERE month = 12 AND year = 2018 AND day = 17), publisher_data(name, cache_status, response_byte) AS (SELECT CASE WHEN position('%' IN url) > 0 THEN split_part(url, '%', 1) ELSE url END, cache_status, response_byte FROM request) SELECT name, count(*) AS total_requests, sum(response_byte) AS total_bytes, sum(case WHEN cache_status = 'hit' THEN 1 ELSE 0 end) AS hit_count, '2018-12-17' AS date FROM publisher_data GROUP BY  name;

	// uriCaseQuery := sq.Case("split_part(clientrequesturi, '/', 2)").
	// 	When("pdf", "split_part(clientrequesturi, '/', 3)").
	// 	Else("split_part(clientrequesturi, '/', 2)")

	// selectQuery := sq.Select().
	// 	Column(uriCaseQuery).
	// 	Columns("cachecachestatus", "edgeresponsebytes").
	// 	From("qt_cloudflare_logs.assettype_com ").
	// 	Where(sq.Eq{"month": month}, sq.Eq{"year": year})
	// fmt.Println(selectQuery.ToSql())
}

func HostQuery(year int, month int, day int) {

}

func UncachedQuery(year int, month int, day int) {

	// query :="SELECT split_part(split_part(request_url,'/', 3), '.', 1) AS publisher_name, count(*) AS total_uncached_requests, '2018-12-17' AS date FROM alb.prod_qtproxy_varnish_internal WHERE year = '2018' AND month = '12' AND day = '17' and request_url IS NOT NULL GROUP BY split_part(split_part(request_url, '/', 3), '.', 1);"

	stringDate := getDateString(year, month, day)

	dateQuery := fmt.Sprint(stringDate, " as date")

	whereClause := sq.And{sq.Eq{"year": strconv.Itoa(year)}, sq.Eq{"month": strconv.Itoa(month)}, sq.Eq{"day": strconv.Itoa(day)}, sq.NotEq{"request_url": nil}}

	query, args, _ := sq.Select().
		Column("split_part(split_part(request_url,'/', 3), '.', 1) as publisher_name").
		Column("count(*) as total_uncached_requests").
		Column(dateQuery).
		From("alb.prod_qtproxy_varnish_internal").
		Where(whereClause).
		GroupBy("split_part(split_part(request_url, '/', 3), '.', 1)").
		PlaceholderFormat(sq.Dollar).
		ToSql()

	fmt.Println(query, args)
}
