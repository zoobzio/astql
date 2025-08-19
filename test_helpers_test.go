package astql

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/zoobzio/sentinel"
)

var initOnce sync.Once

// initSentinel initializes sentinel admin exactly once per process.
// This is idempotent and safe to call from multiple tests.
func initSentinel() {
	initOnce.Do(func() {
		ctx := context.Background()
		// Create the singleton admin instance
		admin, err := sentinel.NewAdmin()
		if err != nil {
			// This should never happen with sync.Once
			panic(fmt.Sprintf("Failed to create sentinel admin: %v", err))
		}

		// Seal the configuration
		// After this, no policy changes are allowed
		if err := admin.Seal(ctx); err != nil {
			panic(fmt.Sprintf("Failed to seal sentinel admin: %v", err))
		}
	})
}

// Test structs for proper registration with sentinel

type User struct {
	Name      string `db:"name"`
	Email     string `db:"email"`
	CreatedAt string `db:"created_at"`
	UpdatedAt string `db:"updated_at"`
	DeletedAt string `db:"deleted_at"`
	ID        int    `db:"id"`
}

type Order struct {
	Status    string `db:"status"`
	OrderDate string `db:"order_date"`
	ID        int    `db:"id"`
	UserID    int    `db:"user_id"`
	Total     int    `db:"total"`
}

type Product struct {
	Name        string `db:"name"`
	Description string `db:"description"`
	ID          int    `db:"id"`
	Price       int    `db:"price"`
	Stock       int    `db:"stock"`
}

// RegisterTestStructs registers all test structs with sentinel.
func RegisterTestStructs(t *testing.T) {
	t.Helper()

	// Ensure sentinel is initialized first
	initSentinel()

	// The field extraction hook in init() should process these automatically
	// when we call Inspect
	ctx := context.Background()

	// Register User struct
	sentinel.Inspect[User](ctx)

	// Register Order struct
	sentinel.Inspect[Order](ctx)

	// Register Product struct
	sentinel.Inspect[Product](ctx)
}

// SetupTest initializes test environment.
func SetupTest(t *testing.T) {
	t.Helper()
	RegisterTestStructs(t)
}
