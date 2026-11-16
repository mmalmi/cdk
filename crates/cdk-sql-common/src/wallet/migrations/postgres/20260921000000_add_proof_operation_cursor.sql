DROP INDEX IF EXISTS proof_created_by_operation_index;
CREATE INDEX IF NOT EXISTS proof_created_by_operation_y_index
    ON proof(created_by_operation, y);
