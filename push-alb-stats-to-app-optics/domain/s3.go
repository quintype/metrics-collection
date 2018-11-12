package domain

import (
	"errors"
	"io"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var fileRegex = regexp.MustCompile(`AWSLogs/\d+/elasticloadbalancing/.+/\d{4}/\d{2}/\d{2}/\d+_elasticloadbalancing_.+_.+.[0-9a-f]+_\d{8}T\d{4}Z_.+_.+.log.gz$`)

// GetAlbLogStream returns a stream to the log file in S3. It will return an error
// if the path does not look like a log file, for whatever reason
func GetAlbLogStream(client s3iface.S3API, bucket, key string) (io.ReadCloser, error) {
	if !fileRegex.MatchString(key) {
		return nil, errors.New("Key does not look like an alb log")
	}

	output, err := client.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})

	if err != nil {
		return nil, err
	}

	return output.Body, nil
}
