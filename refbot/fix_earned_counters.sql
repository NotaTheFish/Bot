SET client_encoding = 'UTF8';
-- Пересчёт счётчиков «заработано/потрачено» из истории (rb_ledger).
-- Раньше они не инкрементились — восстанавливаем задним числом.

-- Заработано грибов (сумма всех положительных начислений)
INSERT INTO rb_counters (tg_id, counter_type, value, updated_at)
SELECT tg_id, 'earned_mush', COALESCE(SUM(delta),0), now()
FROM rb_ledger WHERE currency='mushrooms' AND delta>0 GROUP BY tg_id
ON CONFLICT (tg_id, counter_type) DO UPDATE SET value=GREATEST(rb_counters.value, EXCLUDED.value);

-- Заработано коинов
INSERT INTO rb_counters (tg_id, counter_type, value, updated_at)
SELECT tg_id, 'earned_coin', COALESCE(SUM(delta),0), now()
FROM rb_ledger WHERE currency='coins' AND delta>0 GROUP BY tg_id
ON CONFLICT (tg_id, counter_type) DO UPDATE SET value=GREATEST(rb_counters.value, EXCLUDED.value);

-- Потрачено грибов (сумма всех списаний)
INSERT INTO rb_counters (tg_id, counter_type, value, updated_at)
SELECT tg_id, 'spent_mush', COALESCE(SUM(-delta),0), now()
FROM rb_ledger WHERE currency='mushrooms' AND delta<0 GROUP BY tg_id
ON CONFLICT (tg_id, counter_type) DO UPDATE SET value=GREATEST(rb_counters.value, EXCLUDED.value);

-- Потрачено коинов
INSERT INTO rb_counters (tg_id, counter_type, value, updated_at)
SELECT tg_id, 'spent_coin', COALESCE(SUM(-delta),0), now()
FROM rb_ledger WHERE currency='coins' AND delta<0 GROUP BY tg_id
ON CONFLICT (tg_id, counter_type) DO UPDATE SET value=GREATEST(rb_counters.value, EXCLUDED.value);

-- Потрачено шимкоинов
INSERT INTO rb_counters (tg_id, counter_type, value, updated_at)
SELECT tg_id, 'spent_shim', COALESCE(SUM(-delta),0), now()
FROM rb_ledger WHERE currency='shimcoins' AND delta<0 GROUP BY tg_id
ON CONFLICT (tg_id, counter_type) DO UPDATE SET value=GREATEST(rb_counters.value, EXCLUDED.value);
