package httpapi

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
)

const specPath = "../../api/openapi.yaml"

// TestOpenAPISpecIsValid fails on schema errors, broken $refs or examples
// that don't match their schema.
func TestOpenAPISpecIsValid(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("invalid spec: %v", err)
	}
}

// TestOpenAPISpecMatchesRoutes keeps the spec and the router in sync in both
// directions: no undocumented endpoint, no documented endpoint that doesn't
// exist.
func TestOpenAPISpecMatchesRoutes(t *testing.T) {
	doc, err := openapi3.NewLoader().LoadFromFile(specPath)
	if err != nil {
		t.Fatalf("load spec: %v", err)
	}
	var documented []string
	for path, item := range doc.Paths.Map() {
		for method := range item.Operations() {
			documented = append(documented, strings.ToUpper(method)+" "+path)
		}
	}
	served := RoutePatterns()
	slices.Sort(documented)
	slices.Sort(served)

	for _, r := range served {
		if !slices.Contains(documented, r) {
			t.Errorf("route %q is served but not documented in api/openapi.yaml", r)
		}
	}
	for _, d := range documented {
		if !slices.Contains(served, d) {
			t.Errorf("api/openapi.yaml documents %q, which the router does not serve", d)
		}
	}
}
