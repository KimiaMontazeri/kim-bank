CREATE SCHEMA kim_bank;

SET SCHEMA 'kim_bank';

DROP TYPE IF EXISTS account_type;
CREATE TYPE account_type AS ENUM('client', 'employee');

DROP TYPE IF EXISTS transaction_type;
CREATE TYPE transaction_type AS ENUM ('deposit', 'withdraw', 'transfer', 'interest');

CREATE TABLE IF NOT EXISTS account
(
    username       VARCHAR(50) PRIMARY KEY,
    account_number BIGSERIAL UNIQUE,
    password       VARCHAR(250),
    firstname      VARCHAR(250),
    lastname       VARCHAR(250),
    national_id    VARCHAR(10),
    date_of_birth  DATE,
    type           account_type,
    interest_rate  BIGINT
);

CREATE TABLE IF NOT EXISTS login_log
(
    username   VARCHAR(50) REFERENCES account (username),
    login_time TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions
(
    id               BIGSERIAL PRIMARY KEY,
    type             transaction_type,
    transaction_time TIMESTAMP,
    from_account     INT REFERENCES account (account_number),
    to_account       INT REFERENCES account (account_number),
    amount           BIGINT
);

CREATE TABLE IF NOT EXISTS latest_balances
(
    account_number BIGSERIAL REFERENCES account (account_number),
    amount         BIGINT
);

CREATE TABLE IF NOT EXISTS snapshot_log
(
    snapshot_id        BIGSERIAL,
    snapshot_timestamp timestamp
);

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- DROP PROCEDURE register(username VARCHAR(50), account_number CHAR(16), password VARCHAR(250), firstname VARCHAR(250), lastname VARCHAR(250), national_id VARCHAR(10), date_of_birth DATE, type account_type, interest_rate BIGINT);

CREATE OR REPLACE PROCEDURE register(username VARCHAR(50), password VARCHAR(250),
                                     firstname VARCHAR(250), lastname VARCHAR(250),
                                     national_id VARCHAR(10), date_of_birth DATE, type account_type,
                                     interest_rate BIGINT)
    LANGUAGE plpgsql AS
$$
DECLARE
    RNDSALT           CHAR(16);
    age               int;
    duplicateUsername VARCHAR(150) := NULL;
    account_num       int;
BEGIN
    IF type = 'employee' then
        interest_rate := 0;
    end IF;

    IF type != 'employee' AND type != 'client' then
        RAISE EXCEPTION 'account type is not valid';
    end if;

    IF password IS NULL THEN
        RAISE EXCEPTION 'password cannot be null';
    END IF;

    SELECT EXTRACT(year FROM AGE(CURRENT_DATE, date_of_birth))
    INTO age;

    IF age < 13 then
        RAISE EXCEPTION 'age cannot be below 13';
    END IF;

    SELECT account.username
    INTO duplicateUsername
    FROM account
    WHERE account.username = register.username;

    IF duplicateUsername IS NOT NULL THEN
        RAISE EXCEPTION 'username already exists!';
    end if;

    RNDSALT := encode(gen_random_bytes(8), 'hex');

    password := CONCAT(RNDSALT, encode(digest(CONCAT(RNDSALT, password), 'sha256'), 'hex'));

    INSERT INTO account
    VALUES (username, default, password, firstname, lastname, national_id, date_of_birth, type, interest_rate);
    INSERT INTO login_log VALUES (username, localtimestamp);

    SELECT account_number
    INTO account_num
    FROM account
    WHERE account.username = register.username;

    INSERT INTO latest_balances
    VALUES (account_num, 0);
END;
$$;

-- CALL register('kim', '1234', 'kimberly', 'smiths', '0123456789', '2002-07-15', 'employee', 1);
-- CALL register('john.j', '1111', 'john', 'johnson', '0111111111', '1997-12-03', 'client', 1);
-- check if age constraint is applied
-- CALL register('john.j', '1111', 'john', 'johnson', '0111111111', '2017-07-15', 'client', 1);
-- CALL register('tim', '0000', 'timothy', 'johnson', '0111112348', '1980-01-01', 'client', 1);

CREATE OR REPLACE FUNCTION IsUserPasswordValid(
    plainPass VARCHAR(250), hashedPassword VARCHAR(250)
)
    RETURNS boolean
    LANGUAGE plpgsql
AS
$$
DECLARE
    salt    VARCHAR(16);
    newHash VARCHAR(64);
    isValid boolean := false;

BEGIN
    salt := substring(hashedPassword, 1, 16);

    newHash := encode(digest(CONCAT(salt, plainPass), 'sha256'), 'hex');

    IF (newHash = substring(hashedPassword, 17)) THEN
        isValid := true;
    END IF;

    RETURN isValid;
END;
$$;

SELECT IsUserPasswordValid('1234', '8c88d50dc8d06dfef6dad9690c1ceaf2e1452527ada4c4280a06eb4c452ffcbb6f5726cbc9671edd');

CREATE OR REPLACE PROCEDURE login(username VARCHAR(50), password VARCHAR(250))
    LANGUAGE plpgsql AS
$$
DECLARE
    result boolean := false;
    pass   VARCHAR(250);
BEGIN
    SELECT account.password
    INTO pass
    FROM account
    WHERE account.username = login.username;

    IF pass IS NOT NULL then
        select isuserpasswordvalid from IsUserPasswordValid(login.password, pass) into result;
    end IF;

    IF result = true then
        INSERT INTO login_log VALUES (username, localtimestamp);
    end if;

    IF result = false then
        RAISE EXCEPTION 'Username or password is not correct!';
    end if;
END
$$;

-- CALL login('kim', '1234');
-- CALL login('john.j', '1111');

CREATE OR REPLACE PROCEDURE deposit(amount BIGINT)
    LANGUAGE plpgsql AS
$$
DECLARE
    curr_username       VARCHAR(50);
    curr_account_number int;
BEGIN
    SELECT username
    FROM login_log
    ORDER BY login_time DESC
    LIMIT 1
    INTO curr_username;

    SELECT account_number
    FROM account
    WHERE account.username = curr_username
    INTO curr_account_number;

    INSERT INTO transactions VALUES (default, 'deposit', localtimestamp, NULL, curr_account_number, deposit.amount);
END;
$$;

-- CALL deposit(100);

CREATE OR REPLACE PROCEDURE withdraw(amount BIGINT)
    LANGUAGE plpgsql AS
$$
DECLARE
    curr_username       VARCHAR(50);
    curr_account_number int;
BEGIN
    SELECT username
    FROM login_log
    ORDER BY login_time DESC
    LIMIT 1
    INTO curr_username;

    SELECT account_number
    FROM account
    WHERE account.username = curr_username
    INTO curr_account_number;

    INSERT INTO transactions VALUES (default, 'withdraw', localtimestamp, curr_account_number, NULL, withdraw.amount);
END;
$$;

-- CALL withdraw(200);

CREATE OR REPLACE PROCEDURE transfer(amount BIGINT, to_account INT)
    LANGUAGE plpgsql AS
$$
DECLARE
    curr_username       VARCHAR(50);
    curr_account_number int;
BEGIN
    SELECT username
    FROM login_log
    ORDER BY login_time DESC
    LIMIT 1
    INTO curr_username;

    SELECT account_number
    FROM account
    WHERE account.username = curr_username
    INTO curr_account_number;

    INSERT INTO transactions
    VALUES (default, 'transfer', localtimestamp, curr_account_number, transfer.to_account, transfer.amount);
END;
$$;

-- CALL transfer(500, 3);
-- CALL login('kim', '1234');

CREATE OR REPLACE PROCEDURE updateBalances()
    LANGUAGE plpgsql AS
$$
DECLARE
    last_snapshot_timestamp timestamp;
    last_snapshot_id        BIGINT;
    t_row                   transactions%rowtype;
    prev_amount             BIGINT;
    new_snapshot_id         BIGINT;
    snapshot_table_name     TEXT;
    curr_user_type          account_type;
BEGIN

    SELECT type
    INTO curr_user_type
    FROM account
    WHERE account.username = (SELECT username
                              FROM login_log
                              ORDER BY login_time DESC
                              LIMIT 1);

    IF curr_user_type = 'employee' THEN
        SELECT snapshot_timestamp, snapshot_id
        FROM snapshot_log
        ORDER BY snapshot_timestamp DESC
        LIMIT 1
        INTO last_snapshot_timestamp, last_snapshot_id;

        FOR t_row IN
            SELECT *
            FROM transactions
            WHERE last_snapshot_timestamp IS NULL
               OR transaction_time > last_snapshot_timestamp
            ORDER BY transaction_time
            LOOP
                IF t_row.type = 'deposit' THEN
                    UPDATE latest_balances
                    SET amount = latest_balances.amount + t_row.amount
                    WHERE latest_balances.account_number = t_row.to_account;
                end if;

                IF t_row.type = 'withdraw' THEN
                    SELECT amount
                    INTO prev_amount
                    FROM latest_balances
                    WHERE latest_balances.account_number = t_row.from_account;

                    IF prev_amount < t_row.amount THEN
                        RAISE NOTICE '% DOES NOT HAVE ENOUGH CREDIT: NEEDS % BUT HAS %',
                            t_row.from_account, t_row.amount, prev_amount;
                        CONTINUE;
                    end if;

                    UPDATE latest_balances
                    SET amount = latest_balances.amount - t_row.amount
                    WHERE latest_balances.account_number = t_row.from_account;
                end if;

                If t_row.type = 'transfer' THEN
                    SELECT amount
                    INTO prev_amount
                    FROM latest_balances
                    WHERE latest_balances.account_number = t_row.from_account;

                    IF prev_amount < t_row.amount THEN
                        RAISE NOTICE '% DOES NOT HAVE ENOUGH CREDIT: NEEDS % BUT HAS %',
                            t_row.from_account, t_row.amount, prev_amount;
                        CONTINUE;
                    end if;

                    UPDATE latest_balances
                    SET amount = latest_balances.amount - t_row.amount
                    WHERE latest_balances.account_number = t_row.from_account;

                    UPDATE latest_balances
                    SET amount = latest_balances.amount + t_row.amount
                    WHERE latest_balances.account_number = t_row.to_account;
                end if;
            end loop;

        INSERT INTO snapshot_log VALUES (default, localtimestamp);
        SELECT snapshot_id
        FROM snapshot_log
        ORDER BY snapshot_id DESC
        LIMIT 1
        INTO new_snapshot_id;

        SELECT concat('snapshot_', new_snapshot_id) INTO snapshot_table_name;
        EXECUTE format('CREATE TABLE %1$s AS TABLE latest_balances', snapshot_table_name);
    end if;

    IF curr_user_type != 'employee' THEN
        RAISE EXCEPTION 'OPERATION ON BALANCE UPDATE IS NOT ALLOWED!';
    end if;
END;
$$;

CALL login('kim', '1234');
CALL deposit(1000);
CALL login('john.j', '1111');
CALL deposit(500);
CALL transfer(20, 3);
CALL withdraw(100);

CALL updateBalances();
CALL withdraw(400);
CALL updateBalances();

CREATE OR REPLACE PROCEDURE checkBalance()
    LANGUAGE plpgsql AS
$$
DECLARE
    curr_account_number BIGINT;
    curr_amount         BIGINT;
    notif               text;
BEGIN
    SELECT account_number
    INTO curr_account_number
    FROM account
    WHERE account.username = (SELECT username
                              FROM login_log
                              ORDER BY login_time DESC
                              LIMIT 1);
    SELECT amount
    INTO curr_amount
    FROM latest_balances
    WHERE latest_balances.account_number = curr_account_number;

    notif = 'ACCOUNT NUMBER ' || curr_account_number ||  ' HAS ' || curr_amount || ' DOLLARS IN THEIR ACCOUNT';
    PERFORM pg_notify('raise_notice', notif);
end;
$$;

CALL checkBalance();

