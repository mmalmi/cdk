CREATE INDEX mint_quote_unissued_cursor ON mint_quote (mint_url, unit, id)
WHERE amount_issued = 0 OR payment_method = 'bolt12';
