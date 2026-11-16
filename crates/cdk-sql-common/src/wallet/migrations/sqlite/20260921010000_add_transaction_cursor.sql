-- Exact wallet scope followed by the stable, exclusive binary cursor.
CREATE INDEX transactions_scope_cursor_index
    ON transactions(mint_url, direction, unit, timestamp, id);
