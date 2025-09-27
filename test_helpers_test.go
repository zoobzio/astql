package astql

import (
	"testing"

	"github.com/zoobzio/sentinel"
)

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

	// Just inspect the structs with sentinel - no registration needed
	sentinel.Inspect[User]()
	sentinel.Inspect[Order]()
	sentinel.Inspect[Product]()
}

// SetupTest initializes test environment.
func SetupTest(t *testing.T) {
	t.Helper()
	RegisterTestStructs(t)
}
