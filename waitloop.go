package waitloop

import (
	"errors"
	"time"
)

// ErrLoopTerminated is sent in the Event if the loop was terminated before an event came through the pipeline
var ErrLoopTerminated = errors.New("loop was terminated")

// ErrTimedOut is sent in the Event to close things out in the event that the listener's TTL was met
var ErrTimedOut = errors.New("wait timed out")

type listener struct {
	Key        string
	Channel    chan Event
	Expiration time.Time
}

// Event is a container for data that may trigger listeners
type Event struct {
	Key   string
	Data  interface{}
	Error error
}

// Loop is the main event loop; Initialize it with New() or NewCustom()
type Loop struct {
	listenerMap       map[string][]listener
	terminated        bool
	terminateChan     chan struct{}
	incomingEvents    chan Event
	incomingListeners chan listener
	defaultTTL        time.Duration
	cleanupTicker     *time.Ticker
}

// LoopOptions is a container for configuration for an event loop
type LoopOptions struct {
	// IncomingChannelSize is the size of the buffer for incoming events
	IncomingChannelSize uint64

	// ListenerChannelSize is the size of the buffer for new listeners
	ListenerChannelSize uint64

	// TTL is the default expiration set on new listeners
	TTL time.Duration

	// CleanupInterval is the interval at which cleanup is run (and expired listeners are pruned)
	CleanupInteval time.Duration
}

// New creates a new default event loop
// This loop has IncomingChannelSize=1024, ListenerChannelSize=1024, TTL=1h, CleanupInterval=5s
func New() *Loop {
	return NewCustom(nil)
}

// NewCustom creates a custom event loop from a LoopOptions object
// NewCustom(nil) is equivalent to calling New()
func NewCustom(options *LoopOptions) *Loop {
	if options == nil {
		options = &LoopOptions{}
	}

	if options.IncomingChannelSize == 0 {
		options.IncomingChannelSize = 1024
	}
	if options.ListenerChannelSize == 0 {
		options.ListenerChannelSize = 1024
	}
	if options.TTL == 0 {
		options.TTL = 1 * time.Hour
	}
	if options.CleanupInteval == 0 {
		options.CleanupInteval = 5 * time.Second
	}

	loop := Loop{
		incomingEvents:    make(chan Event, options.IncomingChannelSize),
		incomingListeners: make(chan listener, options.ListenerChannelSize),
		defaultTTL:        options.TTL,
		listenerMap:       map[string][]listener{},
		terminated:        false,
		terminateChan:     make(chan struct{}, 1),
		cleanupTicker:     time.NewTicker(options.CleanupInteval),
	}

	go loop.run()

	return &loop
}

// Wait registers a new listener, and returns a channel on which the Event will arrive
func (l *Loop) Wait(key string) <-chan Event {
	return l.WaitTTL(key, l.defaultTTL)
}

// WaitTTL registers a new listener, and returns a channel on which the Event will arrive
// the TTL argument overrides the configured TTL of the loop
func (l *Loop) WaitTTL(key string, ttl time.Duration) <-chan Event {
	lis := listener{
		Key:        key,
		Expiration: time.Now().Add(l.defaultTTL),
		Channel:    make(chan Event),
	}
	if !l.terminated {
		l.incomingListeners <- lis
	} else {
		go func() {
			lis.Channel <- Event{Key: key, Error: ErrLoopTerminated}
			close(lis.Channel)
		}()
	}
	return lis.Channel
}

// Send receives an Event and triggers any listeners with its key
func (l *Loop) Send(e Event) {
	if l.terminated {
		return
	}
	l.incomingEvents <- e
}

// Terminate stops the event loop and cancels any listeners
func (l *Loop) Terminate() {
	l.terminateChan <- struct{}{}
}

func (l *Loop) run() {
	for !l.terminated {
		select {
		case <-l.terminateChan:
			l.terminated = true
		case lis := <-l.incomingListeners:
			l.registerListener(lis)
		case e := <-l.incomingEvents:
			l.processEvent(e)
		case <-l.cleanupTicker.C:
			l.cleanup()
		}
	}
	l.terminate()
}

func (l *Loop) registerListener(lis listener) {
	if _, ok := l.listenerMap[lis.Key]; !ok {
		l.listenerMap[lis.Key] = []listener{lis}
	} else {
		l.listenerMap[lis.Key] = append(l.listenerMap[lis.Key], lis)
	}
}

func (l *Loop) processEvent(e Event) {
	waiters, ok := l.listenerMap[e.Key]
	if !ok {
		return
	}
	delete(l.listenerMap, e.Key)

	for _, w := range waiters {
		if w.Expiration.Before(time.Now()) {
			continue
		}
		go func(capturedW listener) {
			capturedW.Channel <- e
			close(capturedW.Channel)
		}(w)
	}
}

func (l *Loop) cleanup() {
	for k := range l.listenerMap {
		for i := range l.listenerMap[k] {
			if time.Now().Before(l.listenerMap[k][i].Expiration) {
				continue
			}

			go func(lis listener) {
				lis.Channel <- Event{Key: k, Error: ErrTimedOut}
				close(lis.Channel)
			}(l.listenerMap[k][i])
			l.listenerMap[k] = append(l.listenerMap[k][:i], l.listenerMap[k][i+1:]...)
		}

		if len(l.listenerMap[k]) == 0 {
			delete(l.listenerMap, k)
		}
	}
}

func (l *Loop) terminate() {
	for k := range l.listenerMap {
		for i := range l.listenerMap[k] {
			go func(lis listener) {
				lis.Channel <- Event{Key: k, Error: ErrLoopTerminated}
				close(lis.Channel)
			}(l.listenerMap[k][i])
			l.listenerMap[k] = append(l.listenerMap[k][:i], l.listenerMap[k][:i+1]...)
		}
		if len(l.listenerMap[k]) == 0 {
			delete(l.listenerMap, k)
		}
	}
}
