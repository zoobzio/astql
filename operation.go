package astql

import "github.com/zoobzio/astql/internal/types"

// Re-export operation constants for public API.
const (
	OpSelect   = types.OpSelect
	OpInsert   = types.OpInsert
	OpUpdate   = types.OpUpdate
	OpDelete   = types.OpDelete
	OpCount    = types.OpCount
	OpListen   = types.OpListen
	OpNotify   = types.OpNotify
	OpUnlisten = types.OpUnlisten
)
