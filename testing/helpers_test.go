package testing

import (
	"errors"
	"testing"
)

// =============================================================================
// TestInstance Tests
// =============================================================================

func TestTestInstance(t *testing.T) {
	instance := TestInstance(t)
	if instance == nil {
		t.Fatal("Expected non-nil instance")
	}

	// Verify tables exist by creating fields
	_ = instance.F("id")       // users.id
	_ = instance.T("users")    // users table
	_ = instance.T("posts")    // posts table
	_ = instance.T("comments") // comments table
	_ = instance.T("orders")   // orders table
	_ = instance.T("products") // products table
}

// =============================================================================
// AssertSQL Tests
// =============================================================================

func TestAssertSQL_Match(t *testing.T) {
	// This should not cause the test to fail
	AssertSQL(t, "SELECT * FROM users", "SELECT * FROM users")
}

// =============================================================================
// AssertParams Tests
// =============================================================================

func TestAssertParams_Match(t *testing.T) {
	// This should not cause the test to fail
	AssertParams(t, []string{"id", "name"}, []string{"id", "name"})
}

func TestAssertParams_EmptySlices(t *testing.T) {
	// Both empty should match
	AssertParams(t, []string{}, []string{})
}

// =============================================================================
// AssertContainsParam Tests
// =============================================================================

func TestAssertContainsParam_Found(t *testing.T) {
	AssertContainsParam(t, []string{"id", "name", "email"}, "name")
}

func TestAssertContainsParam_FirstElement(t *testing.T) {
	AssertContainsParam(t, []string{"id", "name", "email"}, "id")
}

func TestAssertContainsParam_LastElement(t *testing.T) {
	AssertContainsParam(t, []string{"id", "name", "email"}, "email")
}

// =============================================================================
// AssertNoError Tests
// =============================================================================

func TestAssertNoError_Nil(t *testing.T) {
	AssertNoError(t, nil)
}

// =============================================================================
// AssertError Tests
// =============================================================================

func TestAssertError_Error(t *testing.T) {
	AssertError(t, errors.New("test error"))
}

// =============================================================================
// AssertErrorContains Tests
// =============================================================================

func TestAssertErrorContains_Match(t *testing.T) {
	AssertErrorContains(t, errors.New("connection failed: timeout"), "timeout")
}

func TestAssertErrorContains_ExactMatch(t *testing.T) {
	AssertErrorContains(t, errors.New("timeout"), "timeout")
}

func TestAssertErrorContains_PartialMatch(t *testing.T) {
	AssertErrorContains(t, errors.New("database connection timeout occurred"), "timeout")
}

// =============================================================================
// AssertPanics Tests
// =============================================================================

func TestAssertPanics_Panics(t *testing.T) {
	AssertPanics(t, func() {
		panic("expected panic")
	})
}

func TestAssertPanics_PanicsWithError(t *testing.T) {
	AssertPanics(t, func() {
		panic(errors.New("error panic"))
	})
}

// =============================================================================
// AssertPanicsWithMessage Tests
// =============================================================================

func TestAssertPanicsWithMessage_StringPanic(t *testing.T) {
	AssertPanicsWithMessage(t, func() {
		panic("invalid input: value too large")
	}, "invalid input")
}

func TestAssertPanicsWithMessage_ErrorPanic(t *testing.T) {
	AssertPanicsWithMessage(t, func() {
		panic(errors.New("validation failed: missing field"))
	}, "validation failed")
}

func TestAssertPanicsWithMessage_ExactMatch(t *testing.T) {
	AssertPanicsWithMessage(t, func() {
		panic("exact message")
	}, "exact message")
}

// =============================================================================
// containsString Tests
// =============================================================================

func TestContainsString_ExactMatch(t *testing.T) {
	if !containsString("hello", "hello") {
		t.Error("containsString should return true for exact match")
	}
}

func TestContainsString_Substring(t *testing.T) {
	if !containsString("hello world", "world") {
		t.Error("containsString should return true when substring exists")
	}
}

func TestContainsString_SubstringAtStart(t *testing.T) {
	if !containsString("hello world", "hello") {
		t.Error("containsString should return true when substring at start")
	}
}

func TestContainsString_EmptySubstring(t *testing.T) {
	if !containsString("hello", "") {
		t.Error("containsString should return true for empty substring")
	}
}

func TestContainsString_NoMatch(t *testing.T) {
	if containsString("hello", "world") {
		t.Error("containsString should return false when substring not found")
	}
}

func TestContainsString_SubstringLonger(t *testing.T) {
	if containsString("hi", "hello") {
		t.Error("containsString should return false when substring is longer")
	}
}

func TestContainsString_EmptyString(t *testing.T) {
	if !containsString("", "") {
		t.Error("containsString should return true for empty string and empty substring")
	}
}

func TestContainsString_EmptyStringNonEmptySubstr(t *testing.T) {
	if containsString("", "hello") {
		t.Error("containsString should return false for empty string with non-empty substring")
	}
}

// =============================================================================
// findSubstring Tests
// =============================================================================

func TestFindSubstring_Found(t *testing.T) {
	if !findSubstring("hello world", "world") {
		t.Error("findSubstring should return true when found")
	}
}

func TestFindSubstring_FoundAtStart(t *testing.T) {
	if !findSubstring("hello world", "hello") {
		t.Error("findSubstring should return true when found at start")
	}
}

func TestFindSubstring_FoundAtEnd(t *testing.T) {
	if !findSubstring("hello world", "world") {
		t.Error("findSubstring should return true when found at end")
	}
}

func TestFindSubstring_FoundInMiddle(t *testing.T) {
	if !findSubstring("hello beautiful world", "beautiful") {
		t.Error("findSubstring should return true when found in middle")
	}
}

func TestFindSubstring_NotFound(t *testing.T) {
	if findSubstring("hello world", "foo") {
		t.Error("findSubstring should return false when not found")
	}
}

func TestFindSubstring_SingleChar(t *testing.T) {
	if !findSubstring("hello", "e") {
		t.Error("findSubstring should find single character")
	}
}

func TestFindSubstring_ExactMatch(t *testing.T) {
	if !findSubstring("hello", "hello") {
		t.Error("findSubstring should return true for exact match")
	}
}
