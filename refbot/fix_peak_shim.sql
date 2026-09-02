SET client_encoding = 'UTF8';
-- Фикс: пиковый счётчик шимкоинов хранился в центах (×100), а должен в целых ШК.
-- Сбрасываем кривые значения — они пересчитаются корректно при след. открытии профиля.
DELETE FROM rb_counters WHERE counter_type = 'max_shim';
-- Также сбросим ошибочно выполненные достижения на этот триггер (перепроверятся)
UPDATE rb_user_achievements ua
SET completed = false, completed_at = NULL, progress = 0
FROM rb_achievements a
WHERE ua.ach_id = a.id
  AND a.trigger_type = 'max_shim'
  AND NOT ua.claimed;   -- уже собранные награды не трогаем (по-честному оставим)
