package notification

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"notification-service/internal/domain"
	"notification-service/internal/templating"
	"notification-service/internal/validation"
)

// TemplateInput is a create-template request.
type TemplateInput struct {
	Name    string
	Channel string
	Body    string
}

// TemplateView is a template plus the variables it requires.
type TemplateView struct {
	domain.Template
	Variables []string
}

var templateName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,99}$`)

// CreateTemplate validates and stores a template. The body is compiled
// here so syntax errors are rejected at creation, not when the first
// notification tries to use it.
func (s *Service) CreateTemplate(ctx context.Context, in TemplateInput) (TemplateView, error) {
	var errs []domain.FieldError
	if !templateName.MatchString(in.Name) {
		errs = append(errs, domain.FieldError{Field: "name",
			Message: "must be 1-100 characters of letters, digits, '_', '.', '-', starting with a letter or digit"})
	}
	if !domain.Channel(in.Channel).Valid() {
		errs = append(errs, domain.FieldError{Field: "channel", Message: "must be one of sms, email, push"})
	}
	compiled, err := templating.Compile(in.Body)
	switch {
	case err != nil:
		errs = append(errs, domain.FieldError{Field: "body", Message: err.Error()})
	case strings.ContainsRune(in.Body, 0):
		errs = append(errs, domain.FieldError{Field: "body", Message: "must not contain the NUL character (U+0000)"})
	}
	if len(errs) > 0 {
		return TemplateView{}, &domain.ValidationError{Fields: errs}
	}

	t, err := s.store.InsertTemplate(ctx, domain.Template{
		ID: s.newID(), Name: in.Name, Channel: domain.Channel(in.Channel), Body: in.Body,
	})
	if err != nil {
		return TemplateView{}, err
	}
	return TemplateView{Template: t, Variables: compiled.Variables}, nil
}

// GetTemplate returns a template and its required variables.
func (s *Service) GetTemplate(ctx context.Context, id uuid.UUID) (TemplateView, error) {
	t, err := s.store.GetTemplate(ctx, id)
	if err != nil {
		return TemplateView{}, err
	}
	compiled, err := templating.Compile(t.Body)
	if err != nil { // stored bodies were compiled on creation
		return TemplateView{}, fmt.Errorf("stored template %s no longer compiles: %w", id, err)
	}
	return TemplateView{Template: t, Variables: compiled.Variables}, nil
}

// templateCache loads each template once per request: a batch of 1000
// items typically references the same template 1000 times.
type templateCache map[uuid.UUID]*cachedTemplate

type cachedTemplate struct {
	tmpl     domain.Template
	compiled *templating.Compiled
}

// applyTemplate fills in.Content by rendering in.TemplateID with
// in.Variables. Problems with the request come back as field errors; only
// infrastructure failures are returned as error.
func (s *Service) applyTemplate(ctx context.Context, in *CreateInput, cache templateCache) ([]domain.FieldError, error) {
	fieldErr := func(field, msg string) []domain.FieldError {
		return []domain.FieldError{{Field: field, Message: msg}}
	}
	if in.TemplateID == nil {
		if in.Variables != nil {
			return fieldErr("variables", "only allowed together with template_id"), nil
		}
		return nil, nil
	}
	if in.Content != "" {
		return fieldErr("content", "must be empty when template_id is given (content is rendered from the template)"), nil
	}
	if validation.ContainsNUL(in.Variables) {
		return fieldErr("variables", "must not contain the NUL character (U+0000)"), nil
	}

	cached, ok := cache[*in.TemplateID]
	if !ok {
		t, err := s.store.GetTemplate(ctx, *in.TemplateID)
		switch {
		case errors.Is(err, domain.ErrNotFound):
			cached = nil
		case err != nil:
			return nil, err
		default:
			compiled, err := templating.Compile(t.Body)
			if err != nil {
				return nil, fmt.Errorf("stored template %s no longer compiles: %w", t.ID, err)
			}
			cached = &cachedTemplate{tmpl: t, compiled: compiled}
		}
		cache[*in.TemplateID] = cached
	}
	if cached == nil {
		return fieldErr("template_id", "template not found"), nil
	}
	if string(cached.tmpl.Channel) != in.Channel {
		return fieldErr("template_id", fmt.Sprintf("template is for channel %q, not %q", cached.tmpl.Channel, in.Channel)), nil
	}

	rendered, err := cached.compiled.Render(in.Variables)
	var missing *templating.MissingVariablesError
	switch {
	case errors.As(err, &missing):
		return fieldErr("variables", "missing required variables: "+strings.Join(missing.Names, ", ")), nil
	case err != nil:
		return fieldErr("variables", err.Error()), nil
	}
	in.Content = rendered
	return nil, nil
}

// validateInput renders the template (if any) and applies the channel rules
// to the final content, so a rendered message that is too long for an SMS is
// rejected exactly like a hand-written one.
func (s *Service) validateInput(ctx context.Context, in *CreateInput, cache templateCache) ([]domain.FieldError, error) {
	errs, err := s.applyTemplate(ctx, in, cache)
	if err != nil {
		return nil, err
	}
	for _, fe := range s.cfg.Rules.Validate(toValidationRequest(*in), s.now()) {
		// With a template problem there is no content to judge; skip the
		// resulting "content is required" noise.
		if len(errs) > 0 && fe.Field == "content" {
			continue
		}
		errs = append(errs, fe)
	}
	return errs, nil
}
