// Package pg implements account.UserService using PostgreSQL.
package pg

import (
	"context"

	"github.com/jackc/pgx"

	"github.com/marselester/distributed-signup"
)

// UserService reprensets a service to store signed up users.
type UserService struct {
	config Config

	pool *pgx.ConnPool
}

// NewUserService returns a UserService which can be configured with config options.
// By default there are 5 max simultaneous Postgres connections, logs are discarded.
func NewUserService(options ...ConfigOption) *UserService {
	s := UserService{
		config: Config{
			host:           "localhost",
			port:           5432,
			maxConnections: 5,
			logger:         &account.NoopLogger{},
		},
	}

	for _, opt := range options {
		opt(&s.config)
	}
	return &s
}

// Open connects to a PostgreSQL DB.
func (s *UserService) Open() error {
	var err error

	connPoolConfig := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     s.config.host,
			Port:     s.config.port,
			Database: s.config.database,
			User:     s.config.user,
			Password: s.config.password,
		},
		MaxConnections: s.config.maxConnections,
		AfterConnect:   prepareSQL,
	}
	s.pool, err = pgx.NewConnPool(connPoolConfig)

	return err
}

// prepareSQL creates the prepared statements for the given connection.
func prepareSQL(conn *pgx.Conn) error {
	q := map[string]string{
		"create":     "INSERT INTO account (id, username) VALUES ($1, $2)",
		"byUsername": "SELECT id FROM account WHERE username=$1",
	}
	for name, sql := range q {
		if _, err := conn.Prepare(name, sql); err != nil {
			return err
		}
	}
	return nil
}

// Close closes db pool of connections.
func (s *UserService) Close() {
	s.pool.Close()
}

// CreateUser creates a user in Postgres.
func (s *UserService) CreateUser(ctx context.Context, u *account.User) error {
	_, err := s.pool.ExecEx(ctx, "create", nil, u.ID, u.Username)
	return err
}

// ByUsername looks up a user by username or returns account.ErrUserNotFound when a user is not found.
func (s *UserService) ByUsername(ctx context.Context, username string) (*account.User, error) {
	u := account.User{Username: username}
	err := s.pool.QueryRowEx(ctx, "byUsername", nil, username).Scan(&u.ID)
	if err == pgx.ErrNoRows {
		err = account.ErrUserNotFound
	}
	return &u, err
}
