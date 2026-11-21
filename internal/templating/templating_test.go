package templating

import (
	"errors"
	"strings"
	"testing"
)

func TestCompile_DiscoversRootVariables(t *testing.T) {
	tests := []struct {
		body string
		want string
	}{
		{"Hello", ""},
		{"Hi {{.first_name}}", "first_name"},
		{"{{.b}} {{.a}} {{.b}}", "a,b"},                         // sorted, deduplicated
		{"{{if .vip}}Gold{{else}}{{.tier}}{{end}}", "tier,vip"}, // both branches
		{"{{if .a}}A{{else if .b}}B{{end}}", "a,b"},             // else if
		{"{{.order.id}}", "order"},                              // nested: top-level key
		{"{{/* note */}}{{- .x -}}", "x"},                       // comments and trim markers
	}
	for _, tt := range tests {
		c, err := Compile(tt.body)
		if err != nil {
			t.Fatalf("Compile(%q): %v", tt.body, err)
		}
		if got := strings.Join(c.Variables, ","); got != tt.want {
			t.Errorf("Compile(%q).Variables = %q, want %q", tt.body, got, tt.want)
		}
	}
}

func TestCompile_Rejects(t *testing.T) {
	for _, body := range []string{"", "   ", "{{.unclosed", "{{end}}", strings.Repeat("x", MaxBodyLength+1)} {
		if _, err := Compile(body); err == nil {
			t.Errorf("Compile(%.20q...) succeeded, want error", body)
		}
	}
}

// Templates are variables and if/else only. Loops, functions, variables
// and nested templates let one request burn the API's CPU or memory, e.g.
// nested {{range}} over a large array or {{printf "%099999999d" 1}}.
func TestCompile_RejectsLogicAndFunctions(t *testing.T) {
	for _, body := range []string{
		"{{range .items}}{{.}}{{end}}",
		`{{printf "%099999999d" 1}}`,
		"{{with .user}}{{.email}}{{end}}",
		"{{call .fn}}",
		"{{len .items}}",
		"{{$x := .greeting}}{{$x}}",
		`{{.a | printf "%s"}}`,
		`{{define "x"}}hi{{end}}{{template "x"}}`,
		"{{if eq .a .b}}same{{end}}",
		"{{if .a}}{{range .b}}{{end}}{{end}}",
		`{{"literal"}}`,
		"{{.}}",
	} {
		_, err := Compile(body)
		if err == nil || !strings.Contains(err.Error(), "only {{.variable}}") {
			t.Errorf("Compile(%q) err = %v, want an unsupported-syntax error", body, err)
		}
	}
}

func TestRender(t *testing.T) {
	c, err := Compile("Hi {{.first_name}}, order {{.order_id}} {{if .express}}ships today{{else}}ships soon{{end}}.")
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Render(map[string]any{"first_name": "Ayşe", "order_id": 42, "express": true}, 1000)
	if err != nil || got != "Hi Ayşe, order 42 ships today." {
		t.Errorf("Render = %q, %v", got, err)
	}
	// false / "" are present values, not missing ones.
	if got, err := c.Render(map[string]any{"first_name": "", "order_id": "x", "express": false}, 1000); err != nil || got != "Hi , order x ships soon." {
		t.Errorf("Render with zero values = %q, %v", got, err)
	}
}

func TestRender_ReportsAllMissingVariables(t *testing.T) {
	c, _ := Compile("{{.a}} {{.b}} {{.c}}")
	_, err := c.Render(map[string]any{"b": 1}, 1000)
	var missing *MissingVariablesError
	if !errors.As(err, &missing) || strings.Join(missing.Names, ",") != "a,c" {
		t.Errorf("err = %v, want missing a,c", err)
	}
	if _, err := c.Render(nil, 1000); !errors.As(err, &missing) || len(missing.Names) != 3 {
		t.Errorf("nil vars err = %v", err)
	}
}

func TestRender_ExtraVariablesAreIgnored(t *testing.T) {
	c, _ := Compile("Hi {{.name}}")
	if got, err := c.Render(map[string]any{"name": "Can", "unused": 1}, 1000); err != nil || got != "Hi Can" {
		t.Errorf("Render = %q, %v", got, err)
	}
}

// Rendering stops at the caller's limit (the channel's), so one variable
// repeated in a body cannot expand a request into megabytes before the
// channel length rules run.
func TestRender_StopsAtTheGivenLimit(t *testing.T) {
	c, err := Compile(strings.Repeat("{{.a}}", 100))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Render(map[string]any{"a": strings.Repeat("x", 10)}, 641); !errors.Is(err, ErrRenderTooLarge) {
		t.Errorf("1000-byte render with a 641-byte limit: err = %v, want ErrRenderTooLarge", err)
	}
	if got, err := c.Render(map[string]any{"a": "x"}, 641); err != nil || len(got) != 100 {
		t.Errorf("small render = %d bytes, %v", len(got), err)
	}
}
