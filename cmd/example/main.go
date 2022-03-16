package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/onflow/flow-go-sdk"
	"github.com/onflow/flow-go-sdk/client"
	poller "github.com/peterargue/flow-event-poller"
	"google.golang.org/grpc"
)

const (
	mainnetAccessNodeURL = "access.mainnet.nodes.onflow.org:9000"
	pollingInterval      = 30 * time.Second
)

var events = []string{
	"A.1654653399040a61.FlowToken.TokensWithdrawn",
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	log.Println("Starting up...")

	client, err := client.New(mainnetAccessNodeURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("error creating gRPC client: %v", err)
	}

	sub := poller.NewEventPoller(client, pollingInterval)
	ch := sub.Subscribe(events)

	go signalHandler(cancel)
	go eventLoop(ctx, ch)

	if err := sub.Run(ctx); err != nil {
		log.Fatalf("error running event poller: %v", err)
	}

	log.Println("Shutting down...")
}

func eventHandler(event *flow.Event) {
	log.Printf("Tx : %s => Event : %s", event.TransactionID, event.ID())
}

func eventLoop(ctx context.Context, ch <-chan *poller.BlockEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-ch:
			eventHandler(e.Event)
		}
	}
}

func signalHandler(cancel context.CancelFunc) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)

	<-sig
	cancel()
}
