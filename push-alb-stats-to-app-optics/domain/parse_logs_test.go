package domain

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsingGzipStreamToRecords(t *testing.T) {
	file, err := os.Open("../test-sample/parse-log-sample.log.gz")
	assert.Nil(t, err)

	ch := ParseLogFile(file)

	v := <-ch

	assert.Equal(t, "app/alb-name/aaaabbbbbccccffff", v.AlbName)
	assert.Equal(t, 1541548200, v.Minute)
	assert.Equal(t, "www.quintype.io", v.Host)
	assert.Equal(t, "80", v.Port)
	assert.Equal(t, float64(0.001), v.RequestProcessingTime)
	assert.Equal(t, 200, v.Status)
	assert.Equal(t, int64(899), v.TotalBytes)
	assert.False(t, v.IsError)

	assert.Nil(t, <-ch)
}

func TestParsingGzipStreamHasFailureForResponseTimeMinusOne(t *testing.T) {
	file, err := os.Open("../test-sample/parse-logs-negative-processing-time.log.gz")
	assert.Nil(t, err)

	ch := ParseLogFile(file)

	v := <-ch

	assert.Equal(t, float64(-1), v.RequestProcessingTime)
	assert.True(t, v.IsError)

	assert.Nil(t, <-ch)
}

func TestParsingGzipStreamHasFailureForStatus500(t *testing.T) {
	file, err := os.Open("../test-sample/parse-logs-status-500.log.gz")
	assert.Nil(t, err)

	ch := ParseLogFile(file)

	v := <-ch

	assert.Equal(t, 500, v.Status)
	assert.True(t, v.IsError)

	assert.Nil(t, <-ch)
}
