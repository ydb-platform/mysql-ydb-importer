package ydb

import (
	"fmt"
	"log"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

// issueSeverity maps a YQL issue severity code to a label (YDB: 0=FATAL, 1=ERROR, 2=WARNING, 3=INFO).
func issueSeverity(severity uint32) string {
	switch severity {
	case 0:
		return "FATAL"
	case 1:
		return "ERROR"
	case 2:
		return "WARNING"
	case 3:
		return "INFO"
	default:
		return fmt.Sprintf("SEVERITY(%d)", severity)
	}
}

// LogIssuesIfDebug prints YQL/operation issues from err when debug is enabled.
func LogIssuesIfDebug(debug bool, err error) {
	if !debug || err == nil {
		return
	}
	if s := IssuesString(err); s != "" {
		log.Printf("YDB YQL issues:\n%s", s)
	}
}

// IssuesString extracts YQL/operation issues from a YDB error (including nested issues) and formats them
// as a multi-line string. Returns "" when the error is nil or carries no issues (e.g. a transport-only or
// non-YDB error such as a MySQL or context error).
func IssuesString(err error) string {
	if err == nil {
		return ""
	}
	var b strings.Builder
	ydbsdk.IterateByIssues(err, func(message string, code Ydb.StatusIds_StatusCode, severity uint32) {
		msg := strings.TrimSpace(message)
		if msg == "" {
			return
		}
		fmt.Fprintf(&b, "  [%s] (code=%d) %s\n", issueSeverity(severity), uint32(code), msg)
	})
	return strings.TrimRight(b.String(), "\n")
}

// issueMessagesString formats YQL issue messages (including nested issues) the same way as IssuesString.
// Used for issues emitted by successful queries via the WithIssuesHandler callback.
func issueMessagesString(issues []*Ydb_Issue.IssueMessage) string {
	var b strings.Builder
	appendIssueMessages(&b, issues)
	return strings.TrimRight(b.String(), "\n")
}

func appendIssueMessages(b *strings.Builder, issues []*Ydb_Issue.IssueMessage) {
	for _, m := range issues {
		if m == nil {
			continue
		}
		if msg := strings.TrimSpace(m.GetMessage()); msg != "" {
			fmt.Fprintf(b, "  [%s] (code=%d) %s\n", issueSeverity(m.GetSeverity()), m.GetIssueCode(), msg)
		}
		appendIssueMessages(b, m.GetIssues())
	}
}

// IssuesHandler returns a query.ExecuteOption that logs YQL issues emitted during a *successful* query
// execution (e.g. warnings or info that don't surface as an error). Attach it to Query/Exec/QueryRow
// calls when -ydb-debug is on. The callback may be invoked more than once per query.
func IssuesHandler() query.ExecuteOption {
	return query.WithIssuesHandler(func(issues []*Ydb_Issue.IssueMessage) {
		if s := issueMessagesString(issues); s != "" {
			log.Printf("YDB YQL issues (from query):\n%s", s)
		}
	})
}
