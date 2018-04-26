package kafka

import (
	"github.com/marselester/distributed-signup"
)

var _ account.SignupService = &SignupService{}
