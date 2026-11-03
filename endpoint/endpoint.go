// Package endpoint is the HTTP side of a query: its parameter and field
// lists, reading a request's parameters against them, writing JSON, and running
// an HTTP server and probing one, as a container's health check does.
package endpoint

import (
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/initiallyqq/data-api-framework/problem"
)

// Parameter is one query parameter of a service's query, which is also one
// argument of the MCP tool that relays it. Each service lists its parameters
// once, in its own package: the HTTP handler accepts exactly those
// names, the tool publishes exactly those arguments with those required
// flags, and the service's contract test checks its OpenAPI file against the
// list.
type Parameter struct {
	Name     string
	Required bool
}

// ParameterNames lists the names of parameters, in order.
func ParameterNames(parameters []Parameter) []string {
	names := make([]string, len(parameters))
	for i, parameter := range parameters {
		names[i] = parameter.Name
	}
	return names
}

// Field is one field of the rows a query returns: its name in the JSON
// object, which is the source column's name, and the comment on that column
// in the DolphinDB source tables, which is what the field means. Each service
// lists its fields once, in its own package, in the order of the
// source columns and of the JSON object; the comments are the source tables'
// own words.
type Field struct {
	Name    string
	Comment string
}

// FieldList renders fields for a tool description as "name (comment), ...".
func FieldList(fields []Field) string {
	items := make([]string, len(fields))
	for i, field := range fields {
		items[i] = field.Name + " (" + field.Comment + ")"
	}
	return strings.Join(items, ", ")
}

// CheckParameters refuses any query parameter the route does not take, so a
// misspelt name fails loudly instead of being ignored.
func CheckParameters(values url.Values, parameters []Parameter) error {
	allowed := ParameterNames(parameters)
	for key := range values {
		if !slices.Contains(allowed, key) {
			return problem.New(problem.UnknownParameter,
				fmt.Sprintf("unknown query parameter %q; this endpoint takes %s", key, strings.Join(allowed, ", "))).
				ForParameter(key)
		}
	}
	return nil
}

// ListParameter reads a parameter that takes several values, given as
// ?x=a&x=b or ?x=a,b: it trims and normalizes each value and drops empty and
// repeated ones, keeping the caller's order.
func ListParameter(values url.Values, name string, normalize func(string) string) []string {
	var items []string
	seen := make(map[string]bool)
	for _, raw := range values[name] {
		for part := range strings.SplitSeq(raw, ",") {
			part = normalize(strings.TrimSpace(part))
			if part == "" || seen[part] {
				continue
			}
			seen[part] = true
			items = append(items, part)
		}
	}
	return items
}
