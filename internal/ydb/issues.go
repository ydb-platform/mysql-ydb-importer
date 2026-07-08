package ydb

import (
	"fmt"
	"log"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	ydbtrace "github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

// IssueLoggingDriverOpts configures IssueLoggingDriver.
type IssueLoggingDriverOpts struct {
	LogIssues bool   // log YQL issues from unary operations (-ydb-debug)
	DumpDir   string // dump BulkUpsert chunks when issues appear (-ydb-dump-failed-chunks)
}

// IssueLoggingDriver returns a ydb Option that logs YQL/operation issues carried by unary operation
// calls (Table service: BulkUpsert, DescribeTable, etc.). This is the only way to surface issues from
// BulkUpsert: that API returns no result object, so there is no per-call issues callback (unlike the
// query service's WithIssuesHandler).
//
// It logs on both success AND failure of every invoke. This matters because BulkUpsert (and other
// idempotent ops) retry on errors: each attempt is a separate invoke, and the informative YQL issues
// often live in intermediate failed attempts, while the final error returned to the caller may be a
// context/timeout error that carries no issues at all — so LogIssuesIfDebug on the returned error
// would miss them.
//
// When DumpDir is set, BulkUpsert chunks with attached BulkUpsertDumpContext are dumped once on the
// first invoke that carries issues (including intermediate retries).
func IssueLoggingDriver(opts IssueLoggingDriverOpts) ydbsdk.Option {
	return ydbsdk.WithTraceDriver(ydbtrace.Driver{
		OnConnInvoke: func(start ydbtrace.DriverConnInvokeStartInfo) func(ydbtrace.DriverConnInvokeDoneInfo) {
			method := string(start.Method)
			return func(info ydbtrace.DriverConnInvokeDoneInfo) {
				issuesStr := traceIssuesString(info.Issues)
				if issuesStr == "" {
					return
				}
				if opts.LogIssues {
					log.Printf("YDB YQL issues (from operation):\n%s", issuesStr)
				}
				if opts.DumpDir != "" && isBulkUpsertDriverMethod(method) && start.Context != nil && *start.Context != nil {
					tryDumpBulkUpsertOnIssues(*start.Context, opts.DumpDir, issuesStr, info.Error)
				}
			}
		},
	})
}

// traceIssuesString formats operation issues carried by a driver trace the same way as IssuesString.
// Each trace.Issue is normally backed by *Ydb_Issue.IssueMessage, so we recurse into nested issues
// when possible (the trace.Issue interface itself does not expose them).
func traceIssuesString(issues []ydbtrace.Issue) string {
	var b strings.Builder
	for _, m := range issues {
		if m == nil {
			continue
		}
		if im, ok := m.(*Ydb_Issue.IssueMessage); ok {
			appendIssueMessages(&b, []*Ydb_Issue.IssueMessage{im})
			continue
		}
		if msg := strings.TrimSpace(m.GetMessage()); msg != "" {
			fmt.Fprintf(&b, "  [%s] (code=%d) %s\n", issueSeverity(m.GetSeverity()), m.GetIssueCode(), msg)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}
