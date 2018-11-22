package domain

import (
	"compress/gzip"
	"encoding/csv"
	"errors"
	"io"
	"log"
	"regexp"
	"strconv"
	"time"
)

// Positions of Fields in the CSV
const (
	TimestampField             = 1
	AlbNameField               = 2
	RequestProcessingTimeField = 6
	StatusField                = 8
	ReceivedBytesField         = 10
	SentBytesField             = 11
	RequestField               = 12
)

func toMinute(s string) (int64, error) {
	t1, err := time.Parse(time.RFC3339, s)

	if err != nil {
		return 0, err
	}

	t := t1.Unix()

	return t - (t % 60), nil
}

var hostRegex = regexp.MustCompile(`https?://(?P<host>.+):(?P<port>\d+)/`)

func toHost(request string) (string, string, error) {
	match := hostRegex.FindStringSubmatch(request)
	if len(match) != 3 {
		return "", "", errors.New("Could Not Parse Host and Port")
	}
	return match[1], match[2], nil
}

func csvRowToLogEntry(record []string) (*LogEntry, error) {
	t, err := toMinute(record[TimestampField])
	if err != nil {
		return nil, err
	}

	host, port, err := toHost(record[RequestField])
	if err != nil {
		return nil, err
	}

	requestProcessingTime, err := strconv.ParseFloat(record[RequestProcessingTimeField], 64)
	if err != nil {
		return nil, err
	}

	status, err := strconv.ParseInt(record[StatusField], 10, 32)
	if err != nil {
		return nil, err
	}

	receivedBytes, err := strconv.ParseInt(record[ReceivedBytesField], 10, 32)
	if err != nil {
		return nil, err
	}

	sentBytes, err := strconv.ParseInt(record[SentBytesField], 10, 32)
	if err != nil {
		return nil, err
	}

	return &LogEntry{
		AlbName:               record[AlbNameField],
		Minute:                int(t),
		Host:                  host,
		Port:                  port,
		RequestProcessingTime: float64(requestProcessingTime),
		Status:                int(status),
		TotalBytes:            receivedBytes + sentBytes,
		IsError:               requestProcessingTime < 0 || status >= 500,
	}, nil
}

// ParseLogFile will take stream for a gzipped log file, and return a channel of entries
func ParseLogFile(file io.ReadCloser) chan *LogEntry {
	ch := make(chan *LogEntry, 100)

	go func() {
		defer file.Close()
		defer close(ch)

		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			panic(err.Error())
		}
		defer gzipReader.Close()

		csvReader := csv.NewReader(gzipReader)
		csvReader.Comma = ' '

		for {
			record, err := csvReader.Read()

			if err == io.EOF {
				return
			} else if err != nil {
				log.Println(err.Error())
				continue
			}

			logEntry, err := csvRowToLogEntry(record)

			if err != nil {
				log.Println(err.Error())
				continue
			}

			ch <- logEntry
		}

	}()

	return ch
}
