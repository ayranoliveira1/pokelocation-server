-- migrate:no-transaction
-- Um token FCM é de um aparelho só: ao trocar de conta no mesmo celular, o ping não pode ir para a conta antiga.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS devices_fcm_token ON devices (fcm_token) WHERE fcm_token IS NOT NULL;
