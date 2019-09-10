package api

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

func SaveAthenaData(s3FileName, dataSource string) {
	s3FilePath := os.Getenv("S3_FILE_PATH")
	appHost := os.Getenv("APP_HOST")

	s3Key := fmt.Sprint(s3FilePath, "/", dataSource, "/", s3FileName, ".csv")
	postURL := fmt.Sprint(appHost, "/api/save-athena-data")

	formData := url.Values{
		"s3-key":     {s3Key},
		"dataSource": {dataSource},
	}

	res, err := http.PostForm(postURL, formData)

	if err != nil {
		fmt.Println(err)
	}

	defer res.Body.Close()
	body, bodyErr := ioutil.ReadAll(res.Body)

	if bodyErr != nil {
		fmt.Println(bodyErr)
	}

	fmt.Println(string(body[:]))
}
