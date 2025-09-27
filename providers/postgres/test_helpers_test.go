package postgres

import (
	"testing"

	"github.com/zoobzio/sentinel"
)

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

	// Just inspect the structs with sentinel - no registration needed
	sentinel.Inspect[User]()
	sentinel.Inspect[Post]()
	sentinel.Inspect[Order]()
	sentinel.Inspect[Customer]()
	sentinel.Inspect[ActiveUser]()
}

// SetupTest initializes test environment.
func SetupTest(t *testing.T) {
	t.Helper()
	RegisterTestStructs(t)
}
