package domain

// LogEntry represents an alb log entry in s3
type LogEntry struct {
	AlbName               string
	Minute                int
	Host                  string
	Port                  string
	RequestProcessingTime float64
	Status                int
	TotalBytes            int64
	IsError               bool
}
