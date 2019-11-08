package utils

import (
	"fmt"
	"regexp"
	"time"
)

func notFutureDate(dateString string) bool {
	currentTime := time.Now().Unix()
	date, _ := time.Parse("2006-01-02", dateString)
	dateUnix := date.Unix()

	return dateUnix <= currentTime
}

func ValidateDate(date string) bool {
	dateRegex := regexp.MustCompile("(20)\\d\\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])")

	if dateRegex.MatchString(date) {
		notFutureDate(date)
	}
	return dateRegex.MatchString(date)
}

func GenerateS3Location(bucketName, s3FilePath string, dataSource string, dateObject map[string]string) string {
	return fmt.Sprint("s3://", bucketName, "/", s3FilePath, "/", dataSource, "/", dateObject["year"], "/", dateObject["month"], "/", dateObject["day"])
}
