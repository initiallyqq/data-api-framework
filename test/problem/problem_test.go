package problem_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/initiallyqq/data-api-framework/problem"
)

func TestAProblemIsAlwaysOneTheCatalogueKnows(t *testing.T) {
	if p := problem.New("NO_SUCH_CODE", "x"); p.Code != problem.InternalError || p.Status != 500 {
		t.Fatalf("an unknown code became %+v", p)
	}
	found := problem.NotFound(problem.CategoryNotFound, "category", []string{"fund"}, []string{"a_share", "b_share"})
	body, _ := json.Marshal(found)
	parsed, ok := problem.Parse(body)
	if !ok || parsed.Code != problem.CategoryNotFound || parsed.Status != 404 || parsed.Detail != "category not found: fund; available: a_share, b_share" ||
		strings.Join(parsed.Available, ",") != "a_share,b_share" ||
		!strings.Contains(string(body), `"error":"category not found`) {
		t.Fatalf("not found = %s / %+v", body, parsed)
	}
	for _, body := range []string{"<html>502</html>", `{"status":502}`, `{"code":"X"}`, `[]`} {
		if _, ok := problem.Parse([]byte(body)); ok {
			t.Fatalf("%s was read as a problem", body)
		}
	}
}
