package sqlutil

import (
	"context"
	"database/sql"
)

type (
	// PreparerContext has a PrepareContext method.
	PreparerContext interface {
		PrepareContext(context.Context, string) (*sql.Stmt, error)
	}

	// QueryerContext has QueryContext and QueryRowContext methods.
	QueryerContext interface {
		QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
		QueryRowContext(context.Context, string, ...interface{}) *sql.Row
	}

	// ExecerContext has an ExecContext method.
	ExecerContext interface {
		ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	}

	DB interface {
		PreparerContext
		QueryerContext
		ExecerContext
		Begin() (*sql.Tx, error)
	}
)
