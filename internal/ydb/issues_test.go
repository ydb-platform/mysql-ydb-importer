package ydb

import (
	"errors"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
)

func TestIssuesString_NoIssues(t *testing.T) {
	if got := IssuesString(nil); got != "" {
		t.Errorf("IssuesString(nil) = %q, want empty", got)
	}
	if got := IssuesString(errors.New("plain non-ydb error")); got != "" {
		t.Errorf("IssuesString(plain error) = %q, want empty", got)
	}
}

func TestIssueSeverity(t *testing.T) {
	cases := map[uint32]string{0: "FATAL", 1: "ERROR", 2: "WARNING", 3: "INFO"}
	for sev, want := range cases {
		if got := issueSeverity(sev); got != want {
			t.Errorf("issueSeverity(%d) = %q, want %q", sev, got, want)
		}
	}
	if got := issueSeverity(99); got != "SEVERITY(99)" {
		t.Errorf("issueSeverity(99) = %q, want SEVERITY(99)", got)
	}
}

func TestIssueMessagesString(t *testing.T) {
	if got := issueMessagesString(nil); got != "" {
		t.Errorf("issueMessagesString(nil) = %q, want empty", got)
	}
	issues := []*Ydb_Issue.IssueMessage{
		{
			Message:   "top warning",
			IssueCode: 100,
			Severity:  2,
			Issues: []*Ydb_Issue.IssueMessage{
				{Message: "nested info", IssueCode: 200, Severity: 3},
				{Message: "   ", Severity: 1}, // blank message is skipped
			},
		},
	}
	got := issueMessagesString(issues)
	for _, want := range []string{
		"[WARNING] (code=100) top warning",
		"[INFO] (code=200) nested info",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("issueMessagesString() = %q, missing %q", got, want)
		}
	}
	if strings.Contains(got, "code=0)") {
		t.Errorf("issueMessagesString() should skip blank messages, got %q", got)
	}
	if strings.HasSuffix(got, "\n") {
		t.Errorf("issueMessagesString() should not have trailing newline, got %q", got)
	}
}
