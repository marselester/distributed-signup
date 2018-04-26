package pg

// UserSchema is db schema which must be created before working with UserService.
const UserSchema = `
CREATE TABLE IF NOT EXISTS account (
    id varchar(27),
    username varchar(40) NOT NULL,

    PRIMARY KEY(id),
    UNIQUE(username)
);
`
