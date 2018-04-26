/*
Command signup-ctl reads usernames from stdin and creates signup requests. All signup responses are printed in stdout.

Every request for a username is encoded as a message, and appended to a partition determined by hash of the username.
*/
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	kitlog "github.com/go-kit/kit/log"
	"github.com/segmentio/ksuid"

	"github.com/marselester/distributed-signup"
	"github.com/marselester/distributed-signup/kafka"
)

func main() {
	broker := flag.String("broker", "127.0.0.1:9092", "Comma separated Kafka brokers to connect to.")
	debug := flag.Bool("debug", false, "Enable debug mode.")
	flag.Parse()

	var logger account.Logger
	if *debug {
		w := kitlog.NewSyncWriter(os.Stderr)
		logger = kitlog.NewLogfmtLogger(w)
	} else {
		logger = &account.NoopLogger{}
	}

	signup := kafka.NewSignupService(
		kafka.WithBrokers(*broker),
		kafka.WithLogger(logger),
	)
	if err := signup.Open(); err != nil {
		log.Fatalf("signup-ctl: failed to connect to Kafka: %v", err)
	}
	defer signup.Close()

	// Listen to Ctrl+C and kill/killall to gracefully stop processing signup requests.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigchan
		cancel()
	}()

	go printResponses(ctx, signup)

	in := userInput(os.Stdin)
	for {
		select {
		case <-ctx.Done():
			logger.Log("level", "debug", "msg", "signup-ctl: exit", "err", ctx.Err())
			return

		case username := <-in:
			if username == "" {
				logger.Log("level", "debug", "msg", "signup-ctl: blank username skipped")
				break
			}

			requestID, err := ksuid.NewRandom()
			if err != nil {
				logger.Log("level", "debug", "msg", "signup-ctl: request id not created", "err", err)
				break
			}

			req := account.SignupRequest{
				ID:       requestID.String(),
				Username: username,
			}
			if err = signup.CreateRequest(ctx, &req); err != nil {
				log.Fatalf("signup-ctl: failed to send signup request: %v", err)
			}
		}
	}
}

// printResponses prints signup responses until ctx is cancelled.
func printResponses(ctx context.Context, signup account.SignupService) {
	err := signup.Responses(ctx, func(resp *account.SignupResponse) {
		var c string
		if resp.Success {
			c = `✅`
		} else {
			c = `❌`
		}
		fmt.Printf("%d:%d %s %s %s\n", resp.Partition, resp.SequenceID, resp.RequestID, resp.Username, c)
	})
	if err != nil {
		log.Fatalf("signup-ctl: failed to print signup responses: %v", err)
	}
}

// userInput reads input from r as a set of lines and writes them into the channel.
// When end of the input is reached, the channel is closed.
func userInput(r io.Reader) <-chan string {
	c := make(chan string)
	input := bufio.NewScanner(r)
	go func() {
		for input.Scan() {
			c <- input.Text()
		}
		close(c)
	}()
	return c
}
