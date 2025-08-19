package astql

import (
	"testing"

	"github.com/zoobzio/astql/internal/types"
)

func TestOperationConstants(t *testing.T) {
	tests := []struct {
		name      string
		operation types.Operation
		expected  string
	}{
		{"Select", types.OpSelect, "SELECT"},
		{"Insert", types.OpInsert, "INSERT"},
		{"Update", types.OpUpdate, "UPDATE"},
		{"Delete", types.OpDelete, "DELETE"},
		{"Count", types.OpCount, "COUNT"},
		{"Listen", types.OpListen, "LISTEN"},
		{"Notify", types.OpNotify, "NOTIFY"},
		{"Unlisten", types.OpUnlisten, "UNLISTEN"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.operation) != tt.expected {
				t.Errorf("Expected %s to be '%s', got '%s'", tt.name, tt.expected, string(tt.operation))
			}
		})
	}
}

func TestOperationType(t *testing.T) {
	// Ensure Operation can be used as a string
	var op types.Operation = "TEST"
	// Type assertion to Operation, not string
	if _, ok := interface{}(op).(types.Operation); !ok {
		t.Error("Should be able to assert to Operation type")
	}
	// Can convert to string
	s := string(op)
	if s != "TEST" {
		t.Error("Should be able to convert Operation to string")
	}
}
