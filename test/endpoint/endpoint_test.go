package endpoint_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/initiallyqq/data-api-framework/endpoint"
	"github.com/initiallyqq/data-api-framework/problem"
)

func TestParametersAreCheckedAndListsRead(t *testing.T) {
	parameters := []endpoint.Parameter{{Name: "exchange", Required: true}, {Name: "asof_time"}}
	if err := endpoint.CheckParameters(url.Values{"exchange": {"a"}, "asof_time": {""}}, parameters); err != nil {
		t.Fatal(err)
	}
	err := endpoint.CheckParameters(url.Values{"exchange": {"a"}, "limit": {"1"}}, parameters)
	refusal, ok := err.(*problem.Details)
	if !ok || refusal.Code != problem.UnknownParameter || refusal.Parameter != "limit" ||
		refusal.Detail != `unknown query parameter "limit"; this endpoint takes exchange, asof_time` {
		t.Fatalf("unknown parameter = %#v", err)
	}
	got := endpoint.ListParameter(url.Values{"x": {" b ,a", "", "B,c,", "a"}}, "x", strings.ToUpper)
	if strings.Join(got, ",") != "B,A,C" {
		t.Fatalf("list = %v, want B,A,C: trimmed, normalized, repeats dropped, order kept", got)
	}
	if fields := endpoint.FieldList(
		[]endpoint.Field{{Name: "a", Comment: "甲"}, {Name: "b", Comment: "乙"}},
	); fields != "a (甲), b (乙)" {
		t.Fatalf("fields = %q", fields)
	}
}
