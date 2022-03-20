# flow-event-poller
This is a simple go module for polling the Flow blockchain for events. You provide a list of events
and a polling interval, and receive a channel on which you can receive your subscribed events.

If you have a very simple use case, check out the [simple example](https://github.com/peterargue/flow-examples/tree/main/examples/secure-grpc) for how to
poll for events directly using the flow-go-sdk.

## Usage

```golang
ctx := context.Background()

client, err := client.New("access.mainnet.nodes.onflow.org:9000", grpc.WithInsecure())
if err != nil {
	log.Fatalf("error creating client pool: %v", err)
}

sub := poller.NewEventPoller(client, 60*time.Second)
ch := sub.Subscribe([]string{
	"A.1654653399040a61.FlowToken.TokensWithdrawn",
})

go func() {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-ch:
			eventHandler(e.Event)
		}
	}
}()

if err := sub.Run(ctx); err != nil {
	log.Fatalf("error running event follower: %v", err)
}
```

Then you implement `eventHandler` to suit your needs.

## Running Example
There is a runnable example implementation in `cmd/example/main.go` which demonstrates how to use this module.
```
go run cmd/example/*.go
```
