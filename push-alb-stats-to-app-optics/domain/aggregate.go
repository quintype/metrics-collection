package domain

import "github.com/forestgiant/sliceutil"

type aggregationKey struct {
	AlbName string
	Host    string
	Minute  int
}

// aggregationValue is aggregate stats for a host by minute
type aggregationValue struct {
	Count      int
	TotalBytes int64
}

// Aggregation is a map from host, minute => totals
type Aggregation map[aggregationKey]aggregationValue

func (m Aggregation) getEntry(albname, host string, minute int) aggregationValue {
	return m[aggregationKey{albname, host, minute}]
}

func (m Aggregation) updateEntry(host string, entry *LogEntry) {
	key := aggregationKey{entry.AlbName, host, entry.Minute}
	aggregateEntry := m[key]
	aggregateEntry.Count++
	aggregateEntry.TotalBytes += entry.TotalBytes
	m[key] = aggregateEntry
}

// ConvertToAppOpticsEvents convert the aggregation to events
// FIXME: Test this
func (m Aggregation) ConvertToAppOpticsEvents() (events []interface{}) {
	for key, value := range m {
		events = append(events, map[string]interface{}{
			"name":       "platform.sketches-internal.bytes-requests",
			"time":       key.Minute,
			"attributes": map[string]bool{"aggregate": true},
			"period":     60,
			"sum":        value.TotalBytes,
			"count":      value.Count,
			"tags": map[string]string{
				"alb-name": key.AlbName,
				"host":     key.Host,
			},
		})
	}
	return
}

// AggregateLogEntries consumes the channel and provides an aggregated result
func AggregateLogEntries(ch chan *LogEntry, importantDomains []string) Aggregation {
	aggregation := make(Aggregation)

	for entry := range ch {
		// Potentially Slow?
		if sliceutil.Contains(importantDomains, entry.Host) {
			aggregation.updateEntry(entry.Host, entry)
		}
		aggregation.updateEntry("total", entry)
	}

	return aggregation
}
