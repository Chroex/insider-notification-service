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
		{`{{printf "%s-%d" .code .n}}`, "code,n"},               // function arguments
		{"{{.order.id}}", "order"},                              // nested: top-level key
		{"{{range .items}}{{.name}}{{end}}", "items"},           // .name is per-item
		{"{{with .user}}{{.email}}{{else}}{{.fallback}}{{end}}", "fallback,user"},
		{"{{$x := .greeting}}{{$x}}", "greeting"},
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

func TestRender(t *testing.T) {
	c, err := Compile("Hi {{.first_name}}, order {{.order_id}} {{if .express}}ships today{{else}}ships soon{{end}}.")
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Render(map[string]any{"first_name": "Ayşe", "order_id": 42, "express": true})
	if err != nil || got != "Hi Ayşe, order 42 ships today." {
		t.Errorf("Render = %q, %v", got, err)
	}
	// false / "" are present values, not missing ones.
	if got, err := c.Render(map[string]any{"first_name": "", "order_id": "x", "express": false}); err != nil || got != "Hi , order x ships soon." {
		t.Errorf("Render with zero values = %q, %v", got, err)
	}
}

func TestRender_ReportsAllMissingVariables(t *testing.T) {
	c, _ := Compile("{{.a}} {{.b}} {{.c}}")
	_, err := c.Render(map[string]any{"b": 1})
	var missing *MissingVariablesError
	if !errors.As(err, &missing) || strings.Join(missing.Names, ",") != "a,c" {
		t.Errorf("err = %v, want missing a,c", err)
	}
	if _, err := c.Render(nil); !errors.As(err, &missing) || len(missing.Names) != 3 {
		t.Errorf("nil vars err = %v", err)
	}
}

func TestRender_ExtraVariablesAreIgnored(t *testing.T) {
	c, _ := Compile("Hi {{.name}}")
	if got, err := c.Render(map[string]any{"name": "Can", "unused": 1}); err != nil || got != "Hi Can" {
		t.Errorf("Render = %q, %v", got, err)
	}
}
