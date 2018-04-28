// Command schema creates user schema in Postgres.
package main

import (
	"flag"
	"log"

	"github.com/facebookgo/flagenv"
	"github.com/jackc/pgx"

	"github.com/marselester/distributed-signup/pg"
)

func main() {
	pgHost := flag.String("pghost", "localhost", "PostgreSQL host to connect to.")
	pgPort := flag.Uint("pgport", 5432, "PostgreSQL port to connect to.")
	pgDatabase := flag.String("pgdatabase", "account", "PostgreSQL database name.")
	pgUser := flag.String("pguser", "account", "PostgreSQL user.")
	pgPassword := flag.String("pgpassword", "swordfish", "PostgreSQL password.")
	// Parse env values.
	flagenv.Parse()
	// Override env values with command line flag values.
	flag.Parse()

	c, err := pgx.Connect(pgx.ConnConfig{
		Host:     *pgHost,
		Port:     uint16(*pgPort),
		Database: *pgDatabase,
		User:     *pgUser,
		Password: *pgPassword,
	})
	if err != nil {
		log.Fatalf("schema: could not establish a connection with PostgreSQL: %v", err)
	}
	defer c.Close()

	_, err = c.Exec(pg.UserSchema)
	if err != nil {
		log.Fatalf("schema: failed to create user schema: %v", err)
	}
}
