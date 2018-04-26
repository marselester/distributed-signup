package account

// Error defines account project's errors.
type Error string

func (e Error) Error() string {
	return string(e)
}

const (
	// ErrUserNotFound error indicates that a user is not found in a UserService.
	ErrUserNotFound = Error("user not found")
)
