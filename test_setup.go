package astql

import (
	"github.com/zoobzio/sentinel"
)

// TestUser is a test model for validation.
type TestUser struct {
	ManagerID *int    `db:"manager_id"`
	DeletedAt *string `db:"deleted_at"`
	Name      string  `db:"name" alias:"user_name,full_name,manager_name,display_name,unique_names"`
	Email     string  `db:"email" alias:"email_address,unique_emails"`
	CreatedAt string  `db:"created_at"`
	UpdatedAt string  `db:"updated_at"`
	ID        int     `db:"id" alias:"user_id,uid,user_count,count"`
	Age       int     `db:"age" alias:"user_age,total_age,avg_age,min_age,max_age,age_group,age_category,age_range,rounded_age,age_power,age_sqrt"`
	Active    bool    `db:"active" alias:"status,status_label,active_status,result"`
}

// TestCustomer is a test model for subquery tests.
type TestCustomer struct {
	Name    string `db:"name"`
	Country string `db:"country"`
	ID      int    `db:"id"`
	Active  bool   `db:"active"`
}

// TestOrder is a test model for subquery tests.
type TestOrder struct {
	ID         int `db:"id"`
	CustomerID int `db:"customer_id"`
	ProductID  int `db:"product_id"`
}

// TestProduct is a test model for subquery tests.
type TestProduct struct {
	Name string `db:"name"`
	ID   int    `db:"id"`
}

// TestPost is a test model for nested subquery tests.
type TestPost struct {
	ID         int `db:"id"`
	UserID     int `db:"user_id"`
	CategoryID int `db:"category_id"`
}

// TestCategory is a test model for nested subquery tests.
type TestCategory struct {
	Name string `db:"name"`
	ID   int    `db:"id"`
}

// TestSession is a test model for Redis session storage.
type TestSession struct {
	ID   string `db:"id"`
	Data string `db:"data"`
}

// TestActiveUser is a test model for Redis set operations.
type TestActiveUser struct {
	UserID string `db:"user_id"`
}

// TestLeaderboard is a test model for Redis sorted set operations.
type TestLeaderboard struct {
	UserID string  `db:"user_id"`
	Score  float64 `db:"score"`
}

// TestDocument is a test model for MinIO object storage.
type TestDocument struct {
	Key                string `db:"key"`
	ContentType        string `db:"content_type"`
	CacheControl       string `db:"cache_control"`
	ContentEncoding    string `db:"content_encoding"`
	ContentDisposition string `db:"content_disposition"`
}

// TestImage is a test model for MinIO image storage.
type TestImage struct {
	Path        string `db:"path"`
	ContentType string `db:"content_type"`
	Width       int    `db:"width"`
	Height      int    `db:"height"`
}

// TestArticle is a test model for OpenSearch document storage.
//
//nolint:govet // field alignment not critical for test models
type TestArticle struct {
	ID          string   `db:"id"`
	Title       string   `db:"title"`
	Content     string   `db:"content"`
	Author      string   `db:"author"`
	Tags        []string `db:"tags"`
	PublishedAt string   `db:"published_at"`
	ViewCount   int      `db:"view_count"`
}

// TestProduct is a test model for OpenSearch e-commerce search.
//
//nolint:govet // field alignment not critical for test models
type TestSearchProduct struct {
	SKU         string  `db:"sku"`
	Name        string  `db:"name"`
	Description string  `db:"description"`
	Price       float64 `db:"price"`
	Category    string  `db:"category"`
	InStock     bool    `db:"in_stock"`
}

// SetupTestModels registers test models with Sentinel for field validation.
func SetupTestModels() {
	// Create and seal admin if not already done
	admin := sentinel.GetAdmin()
	if admin == nil {
		admin, _ = sentinel.NewAdmin() //nolint:errcheck // Admin creation is expected to succeed in tests
		admin.Seal()
	}

	// Register the 'alias' tag for extraction
	sentinel.Tag("alias")

	// This will trigger field extraction via our init() hook
	sentinel.Inspect[TestUser]()
	sentinel.Inspect[TestCustomer]()
	sentinel.Inspect[TestOrder]()
	sentinel.Inspect[TestProduct]()
	sentinel.Inspect[TestPost]()
	sentinel.Inspect[TestCategory]()
	sentinel.Inspect[TestSession]()
	sentinel.Inspect[TestActiveUser]()
	sentinel.Inspect[TestLeaderboard]()
	sentinel.Inspect[TestDocument]()
	sentinel.Inspect[TestImage]()
	sentinel.Inspect[TestArticle]()
	sentinel.Inspect[TestSearchProduct]()
}
