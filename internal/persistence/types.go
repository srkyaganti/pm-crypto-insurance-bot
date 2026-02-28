package persistence

import "time"

type EventType string

const (
	EventSystem    EventType = "SYSTEM_EVENT"
	EventConnector EventType = "CONNECTOR_EVENT"
	EventState     EventType = "STATE_EVENT"
	EventRisk      EventType = "RISK_EVENT"
	EventDecision  EventType = "DECISION_EVENT"
	EventExecution EventType = "EXECUTION_EVENT"
)

type Event struct {
	Timestamp time.Time              `json:"ts"`
	Component string                 `json:"component"`
	EventType EventType              `json:"event_type"`
	Message   string                 `json:"msg,omitempty"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
}

func NewEvent(component string, eventType EventType, message string) *Event {
	return &Event{
		Timestamp: time.Now().UTC(),
		Component: component,
		EventType: eventType,
		Message:   message,
		Payload:   make(map[string]interface{}),
	}
}

func (e *Event) WithPayload(key string, value interface{}) *Event {
	if e.Payload == nil {
		e.Payload = make(map[string]interface{})
	}
	e.Payload[key] = value
	return e
}

func (e *Event) WithPayloads(fields map[string]interface{}) *Event {
	if e.Payload == nil {
		e.Payload = make(map[string]interface{})
	}
	for k, v := range fields {
		e.Payload[k] = v
	}
	return e
}

type WriterStats struct {
	EventsWritten int64
	EventsDropped int64
	BytesWritten  int64
	CurrentFile   string
	LastFlushTime time.Time
	RotationCount int64
}

type writerStatsInternal struct {
	eventsWritten int64
	eventsDropped int64
	bytesWritten  int64
	lastFlushNano int64
}
