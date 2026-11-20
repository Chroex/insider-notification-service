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

// Compiled is a parsed template plus the variables it needs.
type Compiled struct {
	tmpl *template.Template
	// Variables are the top-level fields the body references ({{.name}}),
	// sorted. All of them must be supplied when rendering.
	Variables []string
}

// Compile parses body and discovers its required variables. Parse errors
// are returned with the template's own line:column position.
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
	vars := map[string]bool{}
	if tmpl.Tree != nil && tmpl.Root != nil {
		collectRootFields(tmpl.Root, vars)
	}
	names := make([]string, 0, len(vars))
	for name := range vars {
		names = append(names, name)
	}
	slices.Sort(names)
	return &Compiled{tmpl: tmpl, Variables: names}, nil
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
	var out strings.Builder
	if err := c.tmpl.Execute(&out, vars); err != nil {
		return "", fmt.Errorf("render template: %w", err)
	}
	return out.String(), nil
}

// collectRootFields records every field read from the template's root data
// ({{.first_name}}, {{if .vip}}, {{printf "%s" .city}}). Inside
// {{range}} and {{with}} the dot is rebound to a different value, so fields
// in those bodies are not root variables and are skipped; the range/with
// pipeline itself ({{range .items}}) is root-level and is collected.
func collectRootFields(node parse.Node, into map[string]bool) {
	switch n := node.(type) {
	case *parse.ListNode:
		if n == nil {
			return
		}
		for _, child := range n.Nodes {
			collectRootFields(child, into)
		}
	case *parse.ActionNode:
		collectPipe(n.Pipe, into)
	case *parse.IfNode:
		collectPipe(n.Pipe, into)
		collectRootFields(n.List, into)
		collectRootFields(n.ElseList, into)
	case *parse.RangeNode:
		collectPipe(n.Pipe, into)
		collectRootFields(n.ElseList, into) // else runs with the root dot
	case *parse.WithNode:
		collectPipe(n.Pipe, into)
		collectRootFields(n.ElseList, into)
	case *parse.TemplateNode:
		collectPipe(n.Pipe, into)
	}
}

func collectPipe(pipe *parse.PipeNode, into map[string]bool) {
	if pipe == nil {
		return
	}
	for _, cmd := range pipe.Cmds {
		for _, arg := range cmd.Args {
			switch a := arg.(type) {
			case *parse.FieldNode:
				into[a.Ident[0]] = true // .order.id needs "order"
			case *parse.ChainNode:
				if f, ok := a.Node.(*parse.FieldNode); ok {
					into[f.Ident[0]] = true
				}
			case *parse.PipeNode:
				collectPipe(a, into)
			}
		}
	}
}
