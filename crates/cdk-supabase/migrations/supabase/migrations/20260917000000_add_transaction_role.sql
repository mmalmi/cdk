ALTER TABLE transactions ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'primary'
    CHECK (role IN ('primary', 'send_refund'));

INSERT INTO schema_info (key, value) VALUES ('schema_version', '11')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
