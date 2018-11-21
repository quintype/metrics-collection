package domain

import "github.com/forestgiant/sliceutil"

type byteAggregationKey struct {
	AlbName string
	Host    string
	Minute  int
}

// ByteAggregationValue is aggregate stats for a host by minute
type ByteAggregationValue struct {
	Count      int
	TotalBytes int64
}

// ByteAggregation is a map from host, minute => totals
type ByteAggregation map[byteAggregationKey]ByteAggregationValue

// GetEntry returns the summarized stats for a host and minute
func (m ByteAggregation) GetEntry(albname, host string, minute int) ByteAggregationValue {
	return m[byteAggregationKey{albname, host, minute}]
}

func (m ByteAggregation) updateTotalBytes(host string, entry *LogEntry) {
	key := byteAggregationKey{entry.AlbName, host, entry.Minute}
	aggregateEntry := m[key]
	aggregateEntry.Count++
	aggregateEntry.TotalBytes += entry.TotalBytes
	m[key] = aggregateEntry
}

// AggregateLogEntries consumes the channel and provides an aggregated result
func AggregateLogEntries(ch chan *LogEntry, importantDomains []string) ByteAggregation {
	byteMap := make(ByteAggregation)

	for entry := range ch {
		// Potentially Slow?
		if sliceutil.Contains(importantDomains, entry.Host) {
			byteMap.updateTotalBytes(entry.Host, entry)
		}
		byteMap.updateTotalBytes("total", entry)
	}

	return byteMap
}
