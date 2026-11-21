// Package templating renders notification templates written in Go's
// text/template syntax, e.g. "Hi {{.first_name}}, your order {{.order_id}}
// has shipped".
//
// (Named templating, not template, so it doesn't shadow text/template.)
package templating

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"text/template"
	"text/template/parse"
	"unicode/utf8"
)

// MaxBodyLength caps a template body (characters).
const MaxBodyLength = 20_000

// MaxRenderedBytes caps a rendered message. Every channel limit is far
// below it; it exists so one variable repeated in a body cannot expand a
// request into megabytes before the channel length rules run.
const MaxRenderedBytes = 64 << 10

// ErrRenderTooLarge is returned when rendering exceeds MaxRenderedBytes.
var ErrRenderTooLarge = fmt.Errorf("rendered content exceeds %d bytes", MaxRenderedBytes)

// Compiled is a parsed template plus the variables it needs.
type Compiled struct {
	tmpl *template.Template
	// Variables are the top-level fields the body references ({{.name}}),
	// sorted. All of them must be supplied when rendering.
	Variables []string
}

// Compile parses body, checks it uses only the supported syntax and
// discovers its required variables. Parse errors are returned with the
// template's own line:column position.
func Compile(body string) (*Compiled, error) {
	if strings.TrimSpace(body) == "" {
		return nil, errors.New("body is required")
	}
	if n := utf8.RuneCountInString(body); n > MaxBodyLength {
		return nil, fmt.Errorf("body must be at most %d characters, got %d", MaxBodyLength, n)
	}
	// missingkey=error is a safety net: Render checks variables up front so
	// it can report all missing ones at once, rather than the first.
	tmpl, err := template.New("body").Option("missingkey=error").Parse(body)
	if err != nil {
		return nil, fmt.Errorf("invalid template: %w", err)
	}
	if len(tmpl.Templates()) > 1 { // {{define}} or {{block}}
		return nil, errUnsupported("{{define}}")
	}
	vars := map[string]bool{}
	if tmpl.Tree != nil {
		if err := inspect(tmpl.Root, vars); err != nil {
			return nil, err
		}
	}
	names := make([]string, 0, len(vars))
	for name := range vars {
		names = append(names, name)
	}
	slices.Sort(names)
	return &Compiled{tmpl: tmpl, Variables: names}, nil
}

// inspect walks the parse tree, accepting only what a notification
// template needs and recording every variable it reads:
//
//	text, {{/* comments */}}, {{.name}}, {{.order.id}}
//	{{if .vip}}...{{else if .new}}...{{else}}...{{end}}
//
// Loops, functions, variables and nested templates are rejected. They are
// not needed to fill in a message, and they let one request burn the
// API's CPU or memory: nested {{range}} over a large array, or
// {{printf "%099999999d" 1}}. Message logic belongs in the caller.
func inspect(node parse.Node, vars map[string]bool) error {
	switch n := node.(type) {
	case *parse.ListNode:
		if n == nil { // an absent {{else}} branch
			return nil
		}
		for _, child := range n.Nodes {
			if err := inspect(child, vars); err != nil {
				return err
			}
		}
		return nil
	case *parse.TextNode, *parse.CommentNode:
		return nil
	case *parse.ActionNode:
		return inspectField(n.Pipe, n, vars)
	case *parse.IfNode:
		if err := inspectField(n.Pipe, n, vars); err != nil {
			return err
		}
		if err := inspect(n.List, vars); err != nil {
			return err
		}
		return inspect(n.ElseList, vars)
	default:
		return errUnsupported(node.String())
	}
}

// inspectField accepts a pipeline that is exactly one field: .name or .a.b.
func inspectField(pipe *parse.PipeNode, at parse.Node, vars map[string]bool) error {
	if pipe == nil || len(pipe.Decl) > 0 || len(pipe.Cmds) != 1 || len(pipe.Cmds[0].Args) != 1 {
		return errUnsupported(at.String())
	}
	field, ok := pipe.Cmds[0].Args[0].(*parse.FieldNode)
	if !ok {
		return errUnsupported(at.String())
	}
	vars[field.Ident[0]] = true // .order.id needs "order"
	return nil
}

func errUnsupported(syntax string) error {
	return fmt.Errorf("unsupported template syntax %s: only {{.variable}} and "+
		"{{if .variable}}...{{else}}...{{end}} are allowed", syntax)
}

// MissingVariablesError lists every required variable not supplied.
type MissingVariablesError struct {
	Names []string
}

func (e *MissingVariablesError) Error() string {
	return "missing template variables: " + strings.Join(e.Names, ", ")
}

// Render executes the template with vars. Every required variable must be
// present (a value of "" or false is fine; absence is the error).
func (c *Compiled) Render(vars map[string]any) (string, error) {
	var missing []string
	for _, name := range c.Variables {
		if _, ok := vars[name]; !ok {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return "", &MissingVariablesError{Names: missing}
	}
	if vars == nil {
		vars = map[string]any{}
	}
	var out cappedWriter
	if err := c.tmpl.Execute(&out, vars); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}
	return out.String(), nil
}

// cappedWriter stops a render at MaxRenderedBytes; text/template aborts
// execution on the first write error.
type cappedWriter struct {
	strings.Builder
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	if w.Len()+len(p) > MaxRenderedBytes {
		return 0, ErrRenderTooLarge
	}
	return w.Builder.Write(p)
}
