-- Расчёт витрины global_metrics за один день.
-- Используется DAG 2_datamart_update: параметр :day — логическая дата запуска (вчерашний день).
-- День пересчитывается целиком: сначала удаляется, затем вставляется заново.
--
-- Правила расчёта:
--   * тестовые аккаунты (номер счёта < 0) исключаются — и отправитель, и получатель;
--   * в оборот идут только выполненные операции (status = 'done'), каждая один раз:
--     у операции в staging несколько строк — по одной на каждый статус;
--   * сумма — по модулю (исходящие переводы в источнике отрицательные), из минимальных
--     единиц валюты (центы, копейки) переводится в основные и по курсу дня — в доллары (код 420);
--   * аккаунт клиента — account_number_to: в account_number_from во всех транзакциях стоит
--     один из двух внутренних бухгалтерских счетов компании (903810, 914810), по нему
--     клиентов не посчитать;
--   * cnt_transactions — число операций; avg_transactions_per_account — среднее число
--     операций на аккаунт клиента; cnt_accounts_make_transactions — уникальные аккаунты клиентов.

DELETE FROM VT260725214E22__DWH.global_metrics
WHERE date_update = CAST(:day AS date);

INSERT INTO VT260725214E22__DWH.global_metrics (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
WITH done_transactions AS (
    SELECT DISTINCT
        operation_id,
        account_number_to,
        currency_code,
        amount
    FROM VT260725214E22__STAGING.transactions
    WHERE transaction_dt >= CAST(:day AS timestamp)
      AND transaction_dt < CAST(:day AS timestamp) + INTERVAL '1 day'
      AND status = 'done'
      AND account_number_from >= 0
      AND account_number_to >= 0
),
usd_rates AS (
    SELECT
        currency_code,
        currency_code_div
    FROM VT260725214E22__STAGING.currencies
    WHERE date_update >= CAST(:day AS timestamp)
      AND date_update < CAST(:day AS timestamp) + INTERVAL '1 day'
      AND currency_code_with = 420
)
SELECT
    CAST(:day AS date)                        AS date_update,
    t.currency_code                           AS currency_from,
    SUM(ABS(t.amount) / 100
        * CASE WHEN t.currency_code = 420 THEN 1 ELSE r.currency_code_div END
    )                                         AS amount_total,
    COUNT(DISTINCT t.operation_id)            AS cnt_transactions,
    COUNT(DISTINCT t.operation_id)
        / COUNT(DISTINCT t.account_number_to) AS avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_to)       AS cnt_accounts_make_transactions
FROM done_transactions t
LEFT JOIN usd_rates r ON r.currency_code = t.currency_code
GROUP BY t.currency_code;
