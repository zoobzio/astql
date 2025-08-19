package postgres

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/zoobzio/sentinel"
)

var initOnce sync.Once

// initSentinel initializes sentinel admin exactly once per process.
func initSentinel() {
	initOnce.Do(func() {
		ctx := context.Background()
		admin, err := sentinel.NewAdmin()
		if err != nil {
			panic(fmt.Sprintf("Failed to create sentinel admin: %v", err))
		}
		if err := admin.Seal(ctx); err != nil {
			panic(fmt.Sprintf("Failed to seal sentinel admin: %v", err))
		}
	})
}

// Test structs that match what the schema tests expect

type User struct {
	ID    int    `db:"id"`
	Name  string `db:"name"`
	Email string `db:"email"`
	Age   int    `db:"age"`
}

type Post struct {
	ID        int    `db:"id"`
	Title     string `db:"title"`
	UserID    int    `db:"user_id"`
	CreatedAt string `db:"created_at"`
}

type Order struct {
	ID         int     `db:"id"`
	CustomerID int     `db:"customer_id"`
	Total      float64 `db:"total"`
	Status     string  `db:"status"`
	CreatedAt  string  `db:"created_at"`
}

type Customer struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

type ActiveUser struct {
	ID int `db:"id"`
}

// RegisterTestStructs registers all test structs with Sentinel.
func RegisterTestStructs(t *testing.T) {
	t.Helper()

	// Ensure sentinel is initialized first
	initSentinel()

	ctx := context.Background()

	// Register all test structs
	sentinel.Inspect[User](ctx)
	sentinel.Inspect[Post](ctx)
	sentinel.Inspect[Order](ctx)
	sentinel.Inspect[Customer](ctx)
	sentinel.Inspect[ActiveUser](ctx)
}

// SetupTest initializes test environment.
func SetupTest(t *testing.T) {
	t.Helper()
	RegisterTestStructs(t)
}
