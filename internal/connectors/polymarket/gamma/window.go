package gamma

import (
	"fmt"
	"time"
)

func CalculateWindowTimestamp(now time.Time, windowMinutes int) int64 {
	windowSeconds := int64(windowMinutes * 60)
	nowUnix := now.Unix()
	return (nowUnix / windowSeconds) * windowSeconds
}

func BuildSlug(prefix string, window string, timestamp int64) string {
	return fmt.Sprintf("%s%s-%d", prefix, window, timestamp)
}

func GetCurrentSlug(prefix string, windowMinutes int) string {
	now := time.Now()
	timestamp := CalculateWindowTimestamp(now, windowMinutes)

	var window string
	switch windowMinutes {
	case 5:
		window = "5m"
	case 15:
		window = "15m"
	default:
		window = fmt.Sprintf("%dm", windowMinutes)
	}

	return BuildSlug(prefix, window, timestamp)
}
