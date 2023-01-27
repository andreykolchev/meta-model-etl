DROP TABLE IF EXISTS PUBLIC.DIM_COUNTERPARTY;
DROP TABLE IF EXISTS PUBLIC.DIM_AGREEMENT;
DROP TABLE IF EXISTS PUBLIC.DIM_ACCOUNT;
DROP TABLE IF EXISTS PUBLIC.FACT_AGREEMENT;

CREATE TABLE PUBLIC.DIM_COUNTERPARTY
(
    COUNTERPARTY_KEY  int NOT NULL PRIMARY KEY,
    COUNTERPARTY_CODE varchar,
    COUNTERPARTY_NAME varchar,
    UPDATED_TIME      DATETIME
);

CREATE TABLE PUBLIC.DIM_AGREEMENT
(
    AGREEMENT_ID  int NOT NULL PRIMARY KEY,
    AGREEMENT_DATE DATETIME,
    AGREEMENT_DESCRIPTION varchar,
    PARTY_CODE varchar,
    COUNTERPARTY_CODE varchar,
    UPDATED_TIME DATETIME
);

CREATE TABLE PUBLIC.DIM_ACCOUNT
(
    ACCOUNT_KEY int NOT NULL PRIMARY KEY,
    ACCOUNT_SHORT_NAME varchar,
    COUNTERPARTY_CODE varchar,
    UPDATED_TIME DATETIME
);

CREATE TABLE PUBLIC.FACT_AGREEMENT
(
    AGREEMENT_ID  int NOT NULL PRIMARY KEY,
    PARTY_KEY int,
    PARTY_ACCOUNT_KEY int,
    COUNTERPARTY_KEY int,
    COUNTERPARTY_ACCOUNT_KEY int
);