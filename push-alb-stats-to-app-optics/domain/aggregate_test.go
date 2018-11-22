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

	logEntries := AggregateLogEntries(ch, []string{"bloombergquint.com"})

	entry := logEntries.getEntry("foobar", "bloombergquint.com", 1234)
	assert.Equal(t, 1, entry.Count)
	assert.Equal(t, int64(50), entry.TotalBytes)

	totalEntry := logEntries.getEntry("foobar", "total", 1234)
	assert.Equal(t, 1, totalEntry.Count)
	assert.Equal(t, int64(50), totalEntry.TotalBytes)

	assert.Equal(t, 2, len(logEntries))
}

func TestItOnlyCreatesEntriesForSpecialDomains(t *testing.T) {
	ch := make(chan *LogEntry, 100)

	go func() {
		defer close(ch)
		ch <- &LogEntry{
			AlbName:               "foobar",
			Minute:                1234,
			Host:                  "unimportantdomain.com",
			Port:                  "80",
			RequestProcessingTime: 0.001,
			Status:                200,
			TotalBytes:            50,
			IsError:               false,
		}
	}()

	logEntries := AggregateLogEntries(ch, []string{})
	assert.Equal(t, 1, len(logEntries))
}

func TestGettingPercentiles(t *testing.T) {
	ch := make(chan *LogEntry, 100)

	go func() {
		defer close(ch)
		for i := 1; i <= 100; i++ {
			ch <- &LogEntry{
				AlbName:               "foobar",
				Minute:                1234,
				Host:                  "bloombergquint.com",
				Port:                  "80",
				RequestProcessingTime: float64(i) * 0.001,
				Status:                200,
				TotalBytes:            50,
				IsError:               false,
			}
		}
	}()

	logEntries := AggregateLogEntries(ch, []string{"bloombergquint.com"})
	entry := logEntries.getEntry("foobar", "bloombergquint.com", 1234)

	responseTimes := entry.SortedResponseTimes()
	assert.Equal(t, float64(50)/1000, responseTimes.GetPercentile(50))
	assert.Equal(t, float64(75)/1000, responseTimes.GetPercentile(75))
	assert.Equal(t, float64(90)/1000, responseTimes.GetPercentile(90))
	assert.Equal(t, float64(95)/1000, responseTimes.GetPercentile(95))
	assert.Equal(t, float64(99)/1000, responseTimes.GetPercentile(99))
	assert.Equal(t, float64(100)/1000, responseTimes.GetPercentile(100))
}
