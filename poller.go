package poller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
)

const DefaultMaxHeightRange = 250

type ErrorBehavior int

const (
	// ErrorBehaviorContinue will log the error, but continue processing events
	ErrorBehaviorContinue ErrorBehavior = iota

	// ErrorBehaviorStop will log the error, and stop processing events
	ErrorBehaviorStop
)

var ErrAbort = fmt.Errorf("polling aborted due to an error")

type EventPoller struct {
	// StartHeight sets the starting height for the event poller. If not set, the latest sealed
	// block height is used
	StartHeight uint64

	// PollingErrorBehavior sets the behavior when errors are encountered while polling for events.
	PollingErrorBehavior ErrorBehavior

	client        *client.Client
	interval      time.Duration
	subscriptions map[string][]*Subscription
}

type BlockEvent struct {
	Event *flow.Event
}

type Subscription struct {
	ID      string
	Channel chan *BlockEvent
	Events  []string
}

func NewEventPoller(client *client.Client, interval time.Duration) *EventPoller {
	return &EventPoller{
		client:        client,
		interval:      interval,
		subscriptions: make(map[string][]*Subscription),
	}
}

// Subscribe creates a subscription for a list of events, and returns a Subscription struct, which
// contains a channel to receive events
func (p *EventPoller) Subscribe(events []string) *Subscription {
	sub := &Subscription{
		ID:      randomString(16),
		Channel: make(chan *BlockEvent),
		Events:  events,
	}

	for _, event := range events {
		p.subscriptions[event] = append(p.subscriptions[event], sub)
	}

	return sub
}

// Unsubscribe removes subscription for all provided events
func (p *EventPoller) Unsubscribe(id string, events []string) {
	for _, event := range events {
		if _, ok := p.subscriptions[event]; !ok {
			continue
		}

		for i, sub := range p.subscriptions[event] {
			if sub.ID == id {
				p.subscriptions[event] = append(p.subscriptions[event][:i], p.subscriptions[event][i+1:]...)
				break
			}
		}

		if len(p.subscriptions[event]) == 0 {
			delete(p.subscriptions, event)
		}
	}
}

// Run runs the event poller
func (p *EventPoller) Run(ctx context.Context) error {
	lastest, err := p.startHeader(ctx)
	if err != nil {
		return fmt.Errorf("error getting start header: %w", err)
	}

	next := time.After(p.interval)
	for {
		select {
		case <-ctx.Done():
			return nil

		case <-next:
			// restart timer immediately so the poller runs approximately every interval instead of
			// every interval plus processing time
			next = time.After(p.interval)

			newLatest, err := p.checkSubscriptions(ctx, lastest)

			// module is shutting down
			if errors.Is(err, ctx.Err()) {
				return nil
			}

			// error during polling, and we're configured to stop
			if errors.Is(err, ErrAbort) {
				return err
			}

			// otherwise, log and continue
			if err != nil {
				log.Println("error polling events: %v", err)
				// Skip updating latest so we don't lose events. The next run will backfill any
				// missed blocks
				continue
			}

			lastest = newLatest
		}
	}
}

func (p *EventPoller) startHeader(ctx context.Context) (*flow.BlockHeader, error) {
	if p.StartHeight > 0 {
		return p.client.GetBlockHeaderByHeight(ctx, p.StartHeight)
	}

	return p.client.GetLatestBlockHeader(ctx, true)
}

func (p *EventPoller) checkSubscriptions(ctx context.Context, lastHeader *flow.BlockHeader) (*flow.BlockHeader, error) {
	latest, err := p.client.GetLatestBlockHeader(ctx, true)

	if err != nil {
		return nil, fmt.Errorf("error getting latest header: %w", err)
	}

	var header *flow.BlockHeader
	for {
		header = latest

		// make sure the block range is not larger than the max, otherwise we'll need to break
		// it up into multiple ranges
		maxHeight := lastHeader.Height + DefaultMaxHeightRange
		if latest.Height > maxHeight {
			header, err = p.client.GetBlockHeaderByHeight(ctx, maxHeight)
			if err != nil {
				return nil, fmt.Errorf("error getting header for height %d: %w", maxHeight, err)
			}
		}

		for eventSub := range p.subscriptions {
			err = p.pollEvents(ctx, lastHeader.Height+1, header, eventSub)
			if err != nil {
				// module is shutting down
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}

				log.Printf("error polling events %s for %d - %d: %v", eventSub, lastHeader.Height+1, header.Height, err)
				if p.PollingErrorBehavior == ErrorBehaviorStop {
					return nil, ErrAbort
				}
			}
		}

		if header.Height == latest.Height {
			break
		}

		lastHeader = header
	}

	return header, nil
}

func (p *EventPoller) pollEvents(ctx context.Context, startHeight uint64, header *flow.BlockHeader, eventType string) error {
	blockEvents, err := p.client.GetEventsForHeightRange(ctx, client.EventRangeQuery{
		Type:        eventType,
		StartHeight: startHeight,
		EndHeight:   header.Height,
	})
	if err != nil {
		return err
	}

	// sent notifications for events
	for _, be := range blockEvents {
		for _, event := range be.Events {
			event := event
			for _, sub := range p.subscriptions[event.Type] {
				subEvent := &BlockEvent{
					Event: &event,
				}

				select {
				case <-ctx.Done():
					return nil
				case sub.Channel <- subEvent:
				}
			}
		}
	}
	return nil
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}

	return string(s)
}
