-- Templates are reusable message bodies rendered with Go text/template
-- ({{.first_name}}). Required variables are derived from the body at
-- render time, so they are not stored separately.
CREATE TABLE templates (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name       text        NOT NULL UNIQUE,
    channel    text        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    body       text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
