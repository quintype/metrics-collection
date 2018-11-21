package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTheTotalCountAndBytes(t *testing.T) {
	ch := make(chan *LogEntry, 100)

	go func() {
		defer close(ch)
		ch <- &LogEntry{
			AlbName:               "foobar",
			Minute:                1234,
			Host:                  "bloombergquint.com",
			Port:                  "80",
			RequestProcessingTime: 0.001,
			Status:                200,
			TotalBytes:            50,
			IsError:               false,
		}
	}()

	logEntries := AggregateLogEntries(ch)

	entry := logEntries.GetEntry("bloombergquint.com", 1234)
	assert.Equal(t, 1, entry.Count)
	assert.Equal(t, int64(50), entry.TotalBytes)

	totalEntry := logEntries.GetEntry("total", 1234)
	assert.Equal(t, 1, totalEntry.Count)
	assert.Equal(t, int64(50), totalEntry.TotalBytes)
}
