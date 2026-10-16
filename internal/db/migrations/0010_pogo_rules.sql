-- Alertas do PoGo: o worker roda as regras ativas e avisa por FCM. filters é o JSON do modo pokemon do /map/lookup.
CREATE TABLE IF NOT EXISTS pogo_rules (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    uuid NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    name       text NOT NULL,
    filters    jsonb NOT NULL,
    scope      text NOT NULL CHECK (scope IN ('near', 'world')),
    lat        double precision,
    lng        double precision,
    radius_km  double precision,
    enabled    boolean NOT NULL DEFAULT true,
    -- quem desligou: NULL = o usuário; 'trainer_required' = o worker, porque o PoGo recusou o filtro shiny
    disabled_reason text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (scope = 'world' OR (lat IS NOT NULL AND lng IS NOT NULL AND radius_km IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS pogo_rules_user ON pogo_rules (user_id, created_at, id);
-- a rodada do worker lê todas as ativas de uma vez
CREATE INDEX IF NOT EXISTS pogo_rules_enabled ON pogo_rules (user_id, created_at) WHERE enabled;
