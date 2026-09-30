-- DDL хранилища для выпускного проекта (Vertica).
-- Схемы VT260725214E22__STAGING и VT260725214E22__DWH созданы ранее, здесь только таблицы.
-- Все команды идемпотентны — файл можно применять повторно.

-- ============================ STAGING ============================

-- Сырые транзакции из источника (PostgreSQL public.transactions), как есть.
-- Одна операция встречается несколько раз — по строке на каждый статус
-- (queued → in_progress → done / chargeback / blocked).
CREATE TABLE IF NOT EXISTS VT260725214E22__STAGING.transactions (
    operation_id        varchar(60)  NOT NULL,
    account_number_from int          NOT NULL,
    account_number_to   int          NOT NULL,
    currency_code       int          NOT NULL,
    country             varchar(30)  NOT NULL,
    status              varchar(30)  NOT NULL,
    transaction_type    varchar(30)  NOT NULL,
    amount              int          NOT NULL,
    transaction_dt      timestamp(3) NOT NULL
)
-- партиция на день: ежедневная загрузка перезаписывает свой день целиком
PARTITION BY transaction_dt::date;

-- Проекция по датам: сортировка по времени транзакции,
-- сегментация по хешу от даты и идентификатора транзакции.
CREATE PROJECTION IF NOT EXISTS VT260725214E22__STAGING.transactions_by_dates AS
SELECT
    operation_id,
    account_number_from,
    account_number_to,
    currency_code,
    country,
    status,
    transaction_type,
    amount,
    transaction_dt
FROM VT260725214E22__STAGING.transactions
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt::date, operation_id) ALL NODES;


-- Курсы валют (PostgreSQL public.currencies).
-- currency_code_div — сколько единиц currency_code_with в одной единице currency_code
-- (в источнике колонка называется currency_with_div).
CREATE TABLE IF NOT EXISTS VT260725214E22__STAGING.currencies (
    date_update        timestamp    NOT NULL,
    currency_code      int          NOT NULL,
    currency_code_with int          NOT NULL,
    currency_code_div  numeric(5,3) NOT NULL
)
PARTITION BY date_update::date;

CREATE PROJECTION IF NOT EXISTS VT260725214E22__STAGING.currencies_by_dates AS
SELECT
    date_update,
    currency_code,
    currency_code_with,
    currency_code_div
FROM VT260725214E22__STAGING.currencies
ORDER BY date_update, currency_code, currency_code_with
SEGMENTED BY HASH(date_update::date, currency_code) ALL NODES;


-- ============================== DWH ==============================

-- Витрина: агрегаты по дням и валютам транзакции.
-- Одна строка на (date_update, currency_from); день пересчитывается целиком.
CREATE TABLE IF NOT EXISTS VT260725214E22__DWH.global_metrics (
    date_update                    date          NOT NULL, -- дата расчёта (день транзакций)
    currency_from                  int           NOT NULL, -- код валюты транзакции
    amount_total                   numeric(18,2) NOT NULL, -- сумма транзакций по валюте, в долларах
    cnt_transactions               int           NOT NULL, -- количество транзакций по валюте
    avg_transactions_per_account   numeric(18,2) NOT NULL, -- средний объём транзакций с аккаунта
    cnt_accounts_make_transactions int           NOT NULL, -- уникальные аккаунты с транзакциями
    CONSTRAINT global_metrics_pk PRIMARY KEY (date_update, currency_from) ENABLED
)
ORDER BY date_update, currency_from
SEGMENTED BY HASH(date_update, currency_from) ALL NODES
PARTITION BY date_update;
