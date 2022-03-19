package main

import (
	"context"
	"fmt"
	"log"
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
	client *client.Client
}

func main() {
	log.Println("Starting up...")

	// create the Access API client
	client, err := client.New(mainnetAccessNodeURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error creating gRPC client: %v", err)
	}

	var lastHeight uint64
	p := &Poller{client: client}

	// poll for events
	for {
		lastHeight, err = p.checkEvents(context.Background(), lastHeight)
		if err != nil {
			log.Fatalf("error polling events: %v", err)
		}

		time.Sleep(pollingInterval)
	}
}

// checkEvents checks for subscribed events for all blocks between lastHeight and the latest sealed block
func (p *Poller) checkEvents(ctx context.Context, lastHeight uint64) (uint64, error) {
	header, err := p.client.GetLatestBlockHeader(ctx, true)
	if err != nil {
		return lastHeight, fmt.Errorf("error getting latest block header: %w", err)
	}

	// on the first run, just return the last block's height
	if lastHeight == 0 {
		return header.Height, nil
	}

	log.Printf("Checking for events from %d to %d", lastHeight+1, header.Height)
	for _, event := range events {
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
