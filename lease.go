package sqlutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pkg/errors"
)

// Lessor is a provider of leases.
// It's a wrapper around a database handle
// that specifies the name of the database's lease-info table,
// and the important column names in that table.
type Lessor struct {
	db ExecerContext

	// Table is the name of the db table holding lease info.
	// The default if this is unspecified is "leases".
	Table string

	// Name is the name of the column in the lease-info table that holds a lease's name.
	// The column must have a string-compatible type (like TEXT).
	// It must be uniquely indexed (and would make a suitable PRIMARY KEY for the table).
	// The default if this is unspecified is "name".
	Name string

	// Exp is the name of the column in the lease-info table that holds a lease's expiration time.
	// The column must have a time.Time-compatible type (like DATETIME).
	// For performance, a non-unique index should be defined on it.
	// The default if this is unspecified is "exp".
	Exp string

	// Key is the name of the column in the lease-info table that holds the per-lease key.
	// It must have a type capable of storing a 32-byte string.
	// The default if this is unspecified is "key".
	Key string
}

const (
	defaultTable = "leases"
	defaultName  = "name"
	defaultExp   = "exp"
	defaultKey   = "key"
)

func NewLessor(db ExecerContext) *Lessor {
	return &Lessor{db: db}
}

func (l *Lessor) tableName() string {
	if l.Table == "" {
		return defaultTable
	}
	return l.Table
}

func (l *Lessor) nameName() string {
	if l.Name == "" {
		return defaultName
	}
	return l.Name
}

func (l *Lessor) expName() string {
	if l.Exp == "" {
		return defaultExp
	}
	return l.Exp
}

func (l *Lessor) keyName() string {
	if l.Key == "" {
		return defaultKey
	}
	return l.Key
}

// Acquire attempts to acquire the lease named `name` from a Lessor.
// This will fail (without blocking) if that lease is already held and unexpired.
// If the lease is acquired,
// it expires at `exp`.
// It is also assigned a unique Key that is required in Renew and Release operations.
func (l *Lessor) Acquire(ctx context.Context, name string, exp time.Time) (*Lease, error) {
	const delQFmt = `DELETE FROM %s WHERE %s < $1`
	delQ := fmt.Sprintf(delQFmt, l.tableName(), l.expName())
	_, err := l.db.ExecContext(ctx, delQ, time.Now())
	if err != nil {
		return nil, errors.Wrap(err, "deleting stale leases")
	}

	var key [16]byte
	_, err = rand.Reader.Read(key[:])
	if err != nil {
		return nil, errors.Wrap(err, "computing key")
	}
	keyHex := hex.EncodeToString(key[:])

	const insQFmt = `INSERT INTO %s (%s, %s, %s) VALUES ($1, $2, $3)`
	insQ := fmt.Sprintf(insQFmt, l.tableName(), l.nameName(), l.expName(), l.keyName())
	_, err = l.db.ExecContext(ctx, insQ, name, exp, keyHex)
	return &Lease{
		Lessor: l,
		Name:   name,
		Exp:    exp,
		Key:    keyHex,
	}, errors.Wrap(err, "inserting into database")
}

// Lease is the type of a lease acquired from a Lessor.
// Its fields are exported so that callers can port a lease between processes.
// (The receiving process copies the sending process's values for Name, Exp, and Key,
// and assigns its own value for Lessor.)
type Lease struct {
	Lessor *Lessor `json:"-"`
	Name   string
	Exp    time.Time
	Key    string
}

// Renew updates the expiration time of the lease.
// It fails if the lease is expired or otherwise not held.
func (l *Lease) Renew(ctx context.Context, exp time.Time) error {
	const updQFmt = `UPDATE %s SET %s = $1 WHERE %s = $2 AND %s = $3 AND %s > $4`
	updQ := fmt.Sprintf(
		updQFmt,
		l.Lessor.tableName(),
		l.Lessor.expName(),
		l.Lessor.nameName(),
		l.Lessor.keyName(),
		l.Lessor.expName(),
	)
	res, err := l.Lessor.db.ExecContext(ctx, updQ, exp, l.Name, l.Key, time.Now())
	if err != nil {
		return errors.Wrap(err, "updating database")
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "counting affected rows")
	}
	if aff == 0 {
		return errors.New("could not renew")
	}
	return nil
}

// Release releases the lease.
func (l *Lease) Release(ctx context.Context) error {
	const delQFmt = `DELETE FROM %s WHERE %s = $1 AND %s = $2`
	delQ := fmt.Sprintf(
		delQFmt,
		l.Lessor.tableName(),
		l.Lessor.nameName(),
		l.Lessor.keyName(),
	)
	_, err := l.Lessor.db.ExecContext(ctx, delQ, l.Name, l.Key)
	return errors.Wrap(err, "deleting from database")
}

// Context produces a context object with a deadline equal to the lease's expiration time.
// Callers should be sure to call the associated cancel function before the context goes out of scope.
// E.g.:
//
//   ctx, cancel := lease.Context(ctx)
//   defer cancel()
func (l *Lease) Context(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithDeadline(ctx, l.Exp)
}
