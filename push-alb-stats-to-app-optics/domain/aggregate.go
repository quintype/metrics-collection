package domain

type byteAggregationKey struct {
	Host   string
	Minute int
}

// ByteAggregationValue is aggregate stats for a host by minute
type ByteAggregationValue struct {
	Count      int
	TotalBytes int64
}

// ByteAggregation is a map from host, minute => totals
type ByteAggregation map[byteAggregationKey]ByteAggregationValue

// AggregateResults has the aggregated results after processing
type AggregateResults struct {
	byteAggregation ByteAggregation
}

// GetEntry returns the summarized stats for a host and minute
func (f *AggregateResults) GetEntry(host string, minute int) ByteAggregationValue {
	return f.byteAggregation[byteAggregationKey{host, minute}]
}

func (m ByteAggregation) updateTotalBytes(host string, entry *LogEntry) {
	key := byteAggregationKey{host, entry.Minute}
	aggregateEntry := m[key]
	aggregateEntry.Count++
	aggregateEntry.TotalBytes += entry.TotalBytes
	m[key] = aggregateEntry
}

// AggregateLogEntries consumes the channel and provides an aggregated result
func AggregateLogEntries(ch chan *LogEntry) *AggregateResults {
	byteMap := make(ByteAggregation)

	for entry := range ch {
		byteMap.updateTotalBytes(entry.Host, entry)
		byteMap.updateTotalBytes("total", entry)
	}

	return &AggregateResults{byteMap}
}
