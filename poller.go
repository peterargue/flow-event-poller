package poller

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
)

type EventPoller struct {
	client    *client.Client
	interval  time.Duration
	eventType string

	eventSubscriptions map[string][]*eventSubscription
}

type BlockEvent struct {
	Event *flow.Event
}

type eventSubscription struct {
	id     string
	ch     chan *BlockEvent
	events []string
}

func NewEventPoller(client *client.Client, interval time.Duration) *EventPoller {
	return &EventPoller{
		client:             client,
		interval:           interval,
		eventSubscriptions: make(map[string][]*eventSubscription),
	}
}

func (p *EventPoller) Subscribe(events []string) <-chan *BlockEvent {
	sub := &eventSubscription{
		id:     randomString(16),
		ch:     make(chan *BlockEvent),
		events: events,
	}

	for _, event := range events {
		p.eventSubscriptions[event] = append(p.eventSubscriptions[event], sub)
	}

	return sub.ch
}

// Unsubscribe removes subscription for all provided events
func (p *EventPoller) Unsubscribe(id string, events []string) {
	for _, event := range events {
		for i, sub := range p.eventSubscriptions[event] {
			if sub.id == id {
				p.eventSubscriptions[event] = append(p.eventSubscriptions[event][:i], p.eventSubscriptions[event][i+1:]...)
				break
			}
		}
	}
}

func (p *EventPoller) Run(ctx context.Context) error {
	lastest, err := p.latestBlockHeader(ctx)
	if err != nil {
		return fmt.Errorf("error getting latest height: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(p.interval):
			header, err := p.latestBlockHeader(ctx)
			if err != nil {
				log.Printf("error getting latest height: %v", err)
				continue
			}

			log.Printf("Polling events for %d - %d", lastest.Height+1, header.Height)
			for eventSub := range p.eventSubscriptions {
				err = p.pollEvents(ctx, lastest.Height, header, eventSub)
				if err != nil {
					log.Printf("error polling events %s for %d-%d: %v", eventSub, lastest.Height, header.Height, err)
				}
			}

			lastest = header
		}
	}
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

	for _, be := range blockEvents {
		for _, event := range be.Events {
			event := event
			for _, sub := range p.eventSubscriptions[event.Type] {
				subEvent := &BlockEvent{
					Event: &event,
				}

				select {
				case <-ctx.Done():
					return nil
				case sub.ch <- subEvent:
				}
			}
		}
	}
	return nil
}

func (p *EventPoller) latestBlockHeader(ctx context.Context) (*flow.BlockHeader, error) {
	header, err := p.client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func randomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}

	return string(s)
}
