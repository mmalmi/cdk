-- Whole-wallet ownership scans use the same stable cursor across all scopes.
CREATE INDEX transactions_global_cursor_index ON transactions(timestamp, id);
