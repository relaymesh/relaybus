package message

import "time"

type Message struct {
	ID            string
	Topic         string
	Timestamp     time.Time
	ContentType   string
	Payload       []byte
	Metadata      map[string]string
	SchemaVersion string
	Key           string
}
