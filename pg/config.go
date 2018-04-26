package pg

import (
	"github.com/marselester/distributed-signup"
)

// Config configures a UserService. Config is set by the ConfigOption
// values passed to NewUserService.
type Config struct {
	host           string
	port           uint16
	database       string
	user           string
	password       string
	maxConnections int

	logger account.Logger
}

// ConfigOption configures how we set up the UserService.
type ConfigOption func(*Config)

// WithHost sets Postgres host to connect to.
func WithHost(host string) ConfigOption {
	return func(c *Config) {
		c.host = host
	}
}

// WithPort sets Postgres port to connect to.
func WithPort(port uint16) ConfigOption {
	return func(c *Config) {
		c.port = port
	}
}

// WithDatabase sets Postgres database name.
func WithDatabase(database string) ConfigOption {
	return func(c *Config) {
		c.database = database
	}
}

// WithUser sets Postgres user.
func WithUser(user string) ConfigOption {
	return func(c *Config) {
		c.user = user
	}
}

// WithPassword sets Postgres password.
func WithPassword(password string) ConfigOption {
	return func(c *Config) {
		c.password = password
	}
}

// WithMaxConnections sets max simultaneous connections to use.
func WithMaxConnections(max int) ConfigOption {
	return func(c *Config) {
		c.maxConnections = max
	}
}

// WithLogger configures a logger to debug interactions with Postgres.
func WithLogger(l account.Logger) ConfigOption {
	return func(c *Config) {
		c.logger = l
	}
}
