-- Cópia de mensagem de grupo: o group_id é só metadado de entrega. Sem FK: a cópia fica na fila mesmo depois de o
-- grupo sumir.
ALTER TABLE envelopes ADD COLUMN group_id uuid;
