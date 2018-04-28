/*
The signup-server checks if a requested username is taken and emits corresponding response message.

Kafka consumer is based on Sarama which does not support automatic consumer-group rebalancing and offset tracking.
The server reads signup request messages from a single partition of "account.signup_request" topic.
Then a username is looked up in a Postgres and resulting message is published in "account.signup_response" topic.

When unexpected Postgres or Kafka error occurs, the server should be restarted from the latest processed offset.
*/
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/facebookgo/flagenv"
	kitlog "github.com/go-kit/kit/log"
	"github.com/segmentio/ksuid"

	"github.com/marselester/distributed-signup"
	"github.com/marselester/distributed-signup/kafka"
	"github.com/marselester/distributed-signup/pg"
)

func main() {
	pgHost := flag.String("pghost", "localhost", "PostgreSQL host to connect to.")
	pgPort := flag.Uint("pgport", 5432, "PostgreSQL port to connect to.")
	pgDatabase := flag.String("pgdatabase", "account", "PostgreSQL database name.")
	pgUser := flag.String("pguser", "account", "PostgreSQL user.")
	pgPassword := flag.String("pgpassword", "swordfish", "PostgreSQL password.")

	broker := flag.String("broker", "127.0.0.1:9092", "Broker address to connect to.")
	partition := flag.Int("partition", 0, "Partition number of account.signup_request topic.")
	offset := flag.Int64("offset", -1, "Offset index of a partition (-1 to start from the newest, -2 from the oldest).")
	debug := flag.Bool("debug", false, "Enable debug mode.")
	// Parse env values.
	flagenv.Parse()
	// Override env values with command line flag values.
	flag.Parse()

	var logger account.Logger
	if *debug {
		w := kitlog.NewSyncWriter(os.Stderr)
		logger = kitlog.NewLogfmtLogger(w)
	} else {
		logger = &account.NoopLogger{}
	}

	user := pg.NewUserService(
		pg.WithHost(*pgHost),
		pg.WithPort(uint16(*pgPort)),
		pg.WithDatabase(*pgDatabase),
		pg.WithUser(*pgUser),
		pg.WithPassword(*pgPassword),
		pg.WithLogger(logger),
	)
	if err := user.Open(); err != nil {
		log.Fatalf("signup: could not establish a connection with PostgreSQL: %v", err)
	}
	defer user.Close()

	signup := kafka.NewSignupService(
		kafka.WithBrokers(*broker),
		kafka.WithRequestPartition(int32(*partition)),
		kafka.WithRequestOffset(*offset),
		kafka.WithLogger(logger),
	)
	if err := signup.Open(); err != nil {
		log.Fatalf("signup: failed to connect to Kafka: %v", err)
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

	err := signup.Requests(ctx, func(req *account.SignupRequest) {
		log.Printf("%d:%d %s %s", req.Partition, req.SequenceID, req.ID, req.Username)
		resp := account.SignupResponse{
			RequestID: req.ID,
			Username:  req.Username,
		}

		u, err := user.ByUsername(ctx, req.Username)
		switch err {
		case account.ErrUserNotFound:
			userID, err := ksuid.NewRandom()
			if err != nil {
				log.Fatalf("signup: user id not created: %v", err)
			}
			u := account.User{
				ID:       userID.String(),
				Username: req.Username,
			}

			if err = user.CreateUser(ctx, &u); err != nil {
				// It shouldn't happen, because events are processed sequentially.
				log.Fatalf("signup: failed to create user: %v", err)
			}
			resp.Success = true
			log.Printf("%q signed up with ID: %s\n", u.Username, u.ID)

		case nil:
			resp.Success = false
			log.Printf("%q already claimed: %s\n", u.Username, u.ID)

		default:
			log.Fatalf("signup: failed to look up user: %v", err)
		}

		if err = signup.CreateResponse(ctx, &resp); err != nil {
			log.Fatalf("signup: failed to write a response: %v", err)
		}
	})

	if err != nil {
		log.Fatalf("signup: failed to read signup requests: %v", err)
	}
}
