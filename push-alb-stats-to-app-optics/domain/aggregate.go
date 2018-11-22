package domain

import (
	"math"
	"sort"

	"github.com/forestgiant/sliceutil"
)

type aggregationKey struct {
	AlbName string
	Host    string
	Minute  int
}

// aggregationValue is aggregate stats for a host by minute
type aggregationValue struct {
	Count         int
	TotalBytes    int64
	responseTimes responseTimes
}

type responseTimes []float64

func (m aggregationValue) SortedResponseTimes() responseTimes {
	sort.Float64s(m.responseTimes)
	return m.responseTimes
}

func (t responseTimes) GetPercentile(percentile int) float64 {
	index := math.Round((float64(percentile) / 100) * float64(len(t)))
	return t[int(index)-1]
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
	aggregateEntry.responseTimes = append(aggregateEntry.responseTimes, entry.RequestProcessingTime)
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
		responseTimes := value.SortedResponseTimes()
		if len(responseTimes) > 100 {
			for _, percentile := range []int{50, 75, 90, 95, 99} {
				events = append(events, map[string]interface{}{
					"name":       "platform.sketches-internal.percentile",
					"time":       key.Minute,
					"attributes": map[string]bool{"aggregate": true},
					"period":     60,
					"value":      responseTimes.GetPercentile(percentile),
					"tags": map[string]string{
						"alb-name":   key.AlbName,
						"host":       key.Host,
						"percentile": string(percentile),
					},
				})
			}
		}
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
