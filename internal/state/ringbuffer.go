package state

import "time"

func NewRingBuffer(capacity int) *RingBuffer {
	return &RingBuffer{
		Buffer:   make([]PriceTick, 0, capacity),
		Capacity: capacity,
		Index:    0,
		Full:     false,
	}
}

func (rb *RingBuffer) Add(tick PriceTick) {
	if rb.Capacity <= 0 {
		return
	}

	if len(rb.Buffer) < rb.Capacity {
		rb.Buffer = append(rb.Buffer, tick)
		rb.Index = len(rb.Buffer) - 1
	} else {
		rb.Index = (rb.Index + 1) % rb.Capacity
		rb.Buffer[rb.Index] = tick
		rb.Full = true
	}
}

func (rb *RingBuffer) LastN(n int) []PriceTick {
	if n <= 0 || len(rb.Buffer) == 0 {
		return []PriceTick{}
	}

	count := n
	if count > len(rb.Buffer) {
		count = len(rb.Buffer)
	}

	result := make([]PriceTick, count)

	if !rb.Full || len(rb.Buffer) < rb.Capacity {
		start := len(rb.Buffer) - count
		copy(result, rb.Buffer[start:])
	} else {
		start := rb.Index - count + 1
		if start < 0 {
			start += rb.Capacity
		}

		for i := 0; i < count; i++ {
			idx := (start + i) % rb.Capacity
			result[i] = rb.Buffer[idx]
		}
	}

	return result
}

func (rb *RingBuffer) All() []PriceTick {
	if len(rb.Buffer) == 0 {
		return []PriceTick{}
	}

	if !rb.Full {
		result := make([]PriceTick, len(rb.Buffer))
		copy(result, rb.Buffer)
		return result
	}

	result := make([]PriceTick, rb.Capacity)
	start := (rb.Index + 1) % rb.Capacity
	for i := 0; i < rb.Capacity; i++ {
		result[i] = rb.Buffer[(start+i)%rb.Capacity]
	}
	return result
}

func (rb *RingBuffer) Len() int {
	return len(rb.Buffer)
}

func (rb *RingBuffer) IsFull() bool {
	return rb.Full
}

func (rb *RingBuffer) Last() (PriceTick, bool) {
	if len(rb.Buffer) == 0 {
		return PriceTick{}, false
	}
	return rb.Buffer[rb.Index], true
}

func (rb *RingBuffer) Clear() {
	rb.Buffer = make([]PriceTick, 0, rb.Capacity)
	rb.Index = 0
	rb.Full = false
}

func (rb *RingBuffer) GetTicksSince(since time.Time) []PriceTick {
	if len(rb.Buffer) == 0 {
		return []PriceTick{}
	}

	var result []PriceTick
	ticks := rb.All()
	for _, tick := range ticks {
		if tick.Timestamp.After(since) || tick.Timestamp.Equal(since) {
			result = append(result, tick)
		}
	}
	return result
}

func (rb *RingBuffer) GetTicksInRange(start, end time.Time) []PriceTick {
	if len(rb.Buffer) == 0 {
		return []PriceTick{}
	}

	var result []PriceTick
	ticks := rb.All()
	for _, tick := range ticks {
		if (tick.Timestamp.After(start) || tick.Timestamp.Equal(start)) &&
			(tick.Timestamp.Before(end) || tick.Timestamp.Equal(end)) {
			result = append(result, tick)
		}
	}
	return result
}
