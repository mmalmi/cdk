ALTER TABLE transactions ADD COLUMN role TEXT NOT NULL DEFAULT 'primary'
    CHECK (role IN ('primary', 'send_refund'));
