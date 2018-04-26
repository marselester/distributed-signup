package pg_test

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/jackc/pgx"

	"github.com/marselester/distributed-signup"
	"github.com/marselester/distributed-signup/pg"
)

// Ensure pg.UserService implements account.UserService.
var _ account.UserService = &pg.UserService{}

// client is a test wrapper for pg.UserService.
type client struct {
	// connConfig contains Postgres connection settings populated from the env.
	connConfig pgx.ConnConfig
	// conn is a connection to a test db to create schema.
	conn *pgx.Conn
	// sysConn is a connection to "postgres" db to drop/create a test db.
	sysConn *pgx.Conn

	user *pg.UserService
}

// newClient returns configured test client.
// It parses Postgres connection settings for a test db.
func newClient() (*client, error) {
	config, err := parsePgEnv()
	if err != nil {
		return nil, err
	}

	c := client{
		connConfig: config,

		user: pg.NewUserService(
			pg.WithHost(config.Host),
			pg.WithPort(config.Port),
			pg.WithDatabase(config.Database),
			pg.WithUser(config.User),
			pg.WithPassword(config.Password),
		),
	}
	return &c, nil
}

// open creates the test database and opens a test client.
func (c *client) open() error {
	var err error

	config := c.connConfig
	config.Database = "postgres"
	if c.sysConn, err = pgx.Connect(config); err != nil {
		return err
	}
	if _, err = c.sysConn.Exec("DROP DATABASE IF EXISTS " + c.connConfig.Database); err != nil {
		return err
	}
	if _, err = c.sysConn.Exec("CREATE DATABASE " + c.connConfig.Database); err != nil {
		return err
	}

	if c.conn, err = pgx.Connect(c.connConfig); err != nil {
		return err
	}
	if _, err = c.conn.Exec(pg.UserSchema); err != nil {
		return err
	}

	return c.user.Open()
}

// close closes client and drops the test database.
func (c *client) close() {
	c.user.Close()
	c.conn.Close()

	c.sysConn.Exec("DROP DATABASE IF EXISTS " + c.connConfig.Database)
	c.sysConn.Close()
}

// parsePgEnv parses the environment into Postgres connection config.
// The following variables are supported:
// - TEST_PGHOST, localhost by default
// - TEST_PGPORT, 5432 by default
// - TEST_PGDATABASE
// - TEST_PGUSER
// - TEST_PGPASSWORD.
func parsePgEnv() (pgx.ConnConfig, error) {
	config := pgx.ConnConfig{
		Host:     os.Getenv("TEST_PGHOST"),
		Database: os.Getenv("TEST_PGDATABASE"),
		User:     os.Getenv("TEST_PGUSER"),
		Password: os.Getenv("TEST_PGPASSWORD"),
	}

	if p := os.Getenv("TEST_PGPORT"); p != "" {
		if port, err := strconv.Atoi(p); err != nil {
			return config, err
		} else {
			config.Port = uint16(port)
		}
	}

	return config, nil
}

// mustOpenClient creates and opens a test client or panics.
func mustOpenClient() *client {
	c, err := newClient()
	if err != nil {
		panic(err)
	}
	if err := c.open(); err != nil {
		panic(err)
	}
	return c
}

func TestCreateUser(t *testing.T) {
	c := mustOpenClient()
	defer c.close()

	ctx := context.Background()
	want := account.User{
		ID:       "0ujzPyRiIAffKhBux4PvQdDqMHY",
		Username: "bob",
	}
	if err := c.user.CreateUser(ctx, &want); err != nil {
		t.Fatal(err)
	}

	got, err := c.user.ByUsername(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}
	if want != *got {
		t.Errorf("CreateUser(%+v) created %+v", want, got)
	}
}

func TestCreateUserDuplicateUsername(t *testing.T) {
	c := mustOpenClient()
	defer c.close()

	ctx := context.Background()
	u := account.User{
		ID:       "0ujzPyRiIAffKhBux4PvQdDqMHY",
		Username: "bob",
	}
	if err := c.user.CreateUser(ctx, &u); err != nil {
		t.Fatal(err)
	}

	u.ID = "123"
	if err := c.user.CreateUser(ctx, &u); err == nil {
		t.Errorf("CreateUser(%+v) must be duplicate username error", u)
	}
}

func TestCreateUserDuplicateID(t *testing.T) {
	c := mustOpenClient()
	defer c.close()

	ctx := context.Background()
	u := account.User{
		ID:       "0ujzPyRiIAffKhBux4PvQdDqMHY",
		Username: "bob",
	}
	if err := c.user.CreateUser(ctx, &u); err != nil {
		t.Fatal(err)
	}

	u.Username = "alice"
	if err := c.user.CreateUser(ctx, &u); err == nil {
		t.Errorf("CreateUser(%+v) must be duplicate ID error", u)
	}
}

func TestByUsername(t *testing.T) {
	c := mustOpenClient()
	defer c.close()

	ctx := context.Background()
	want := account.User{
		ID:       "0ujzPyRiIAffKhBux4PvQdDqMHY",
		Username: "bob",
	}
	if err := c.user.CreateUser(ctx, &want); err != nil {
		t.Fatal(err)
	}

	got, err := c.user.ByUsername(ctx, "bob")
	if err != nil {
		t.Fatal(err)
	}

	if want != *got {
		t.Errorf("ByUsername(bob) = %+v, wanted %+v", got, want)
	}
}

func TestByUsernameNotFound(t *testing.T) {
	c := mustOpenClient()
	defer c.close()

	ctx := context.Background()
	u, err := c.user.ByUsername(ctx, "bob")
	if err != account.ErrUserNotFound {
		t.Errorf("ByUsername(bob) = %+v, must be ErrUserNotFound", u)
	}
}
