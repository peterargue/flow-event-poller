package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	"google.golang.org/grpc"
)

const (
	mainnetAccessNodeURL = "access.mainnet.nodes.onflow.org:9000"
	pollingInterval      = 30 * time.Second
)

var events = []string{
	"A.1654653399040a61.FlowToken.TokensWithdrawn",
	"A.1654653399040a61.FlowToken.TokensDeposited",
}

type Poller struct {
	client   *client.Client
	events   []string
	interval time.Duration
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	log.Println("Starting up...")

	// create the Access API client
	client, err := client.New(mainnetAccessNodeURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error creating gRPC client: %v", err)
	}

	// register shutdown handler
	go signalHandler(cancel)

	p := &Poller{
		client:   client,
		events:   events,
		interval: pollingInterval,
	}

	// poll for events
	var lastHeight uint64
	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down...")
			return
		case <-time.After(p.interval):
			lastHeight, err = p.checkEvents(ctx, lastHeight)
		}
	}
}

// checkEvents checks for subscribed events for all blocks between lastHeight and the latest sealed block
func (p *Poller) checkEvents(ctx context.Context, lastHeight uint64) (uint64, error) {
	header, err := p.client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return lastHeight, fmt.Errorf("error getting latest block header: %w", err)
	}

	// on the first run, return the last block's events
	if lastHeight == 0 {
		lastHeight = header.Height - 1
	}

	for _, event := range p.events {
		err := p.pollEvent(ctx, lastHeight+1, header.Height, event)

		if err != nil {
			return lastHeight, fmt.Errorf("error polling events: %w", err)
		}
	}

	return header.Height, nil
}

// pollEvents polls for a specific event during a specific block range, and calls eventHandler for
// each event found.
func (p *Poller) pollEvent(ctx context.Context, startHeight uint64, endHeight uint64, eventType string) error {
	blockEvents, err := p.client.GetEventsForHeightRange(ctx, client.EventRangeQuery{
		Type:        eventType,
		StartHeight: startHeight,
		EndHeight:   endHeight,
	})
	if err != nil {
		return err
	}

	// sent notifications for events
	for _, be := range blockEvents {
		for _, event := range be.Events {
			eventHandler(&event)
		}
	}
	return nil
}

// This would contain any event processing logic you need
func eventHandler(event *flow.Event) {
	log.Printf("Tx : %s => %s : %s", event.TransactionID, event.Type, event.ID())
}

// signalHandler listens for SIGINT and SIGTERM and cancels the main context to begin the shutdown
// process
func signalHandler(cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
	cancel()
}
