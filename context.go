package sqlutil

import "context"

type ctxkeytype string

var ctxkey = ctxkeytype("db")

// WithDB creates a child of the given context object containing a DB.
// The DB in the context can be retrieved with GetDB.
func WithDB(ctx context.Context, db DB) context.Context {
	return context.WithValue(ctx, ctxkey, db)
}

// GetDB extracts the DB previously stored in ctx
// (or some parent of ctx)
// with WithDB.
func GetDB(ctx context.Context) DB {
	return ctx.Value(ctxkey).(DB)
}
