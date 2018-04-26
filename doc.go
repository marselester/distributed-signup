/*
Package account defines a domain of user accounts service.

Users sign up at Account service which requires a username.
There are so many people willing to register, that a single PostgreSQL database can't hold all account records,
but three servers are enough for this hypothetical service load.
Therefore we should split (partition) user accounts on three databases and make sure
a username is unique across all of them.
*/
package account
