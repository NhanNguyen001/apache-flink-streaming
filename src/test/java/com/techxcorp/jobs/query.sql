SELECT
    POSTINGINSTRUCTION.SYM_RUN_DATE,
    BRANCH_NO,
    CLIENT_NO,
    CLIENT_NAME,
    POSTINGINSTRUCTION.ACCT_NO,
    ACCT_TYPE,
    ACCT_TYPE_DESC,
    CURRENCY,
    TRAN_DATE,
    TRAN_TYPE,
    TRANSACTION_NO,
    POST_DATE,
    POST_STATUS,
    REVERSAL_TRAN_TYPE,
    REVERSAL_DATE,
    SOURCE_TYPE,
    GL_CODE,
    (
        SELECT
            SODU_NT
        FROM
            ACCOUNTBALANCE
        WHERE
            ACCOUNTBALANCE.ACCOUNT_ID = POSTINGINSTRUCTION.ACCT_NO
            AND ACCOUNTBALANCE.VALUE_TIME < POSTINGINSTRUCTION.VALUE_TIMESTAMP
        ORDER BY
            ACCOUNTBALANCE.VALUE_TIME DESC LIMIT 1
    ) AS PREVIOUS_BAL_AMT,
    DEBIT_CREDIT_IND,
    PSNO,
    PSCO,
    (
        SELECT
            SODU_NT
        FROM
            ACCOUNTBALANCE
        WHERE
            ACCOUNTBALANCE.ACCOUNT_ID = POSTINGINSTRUCTION.ACCT_NO
            AND ACCOUNTBALANCE.VALUE_TIME > POSTINGINSTRUCTION.VALUE_TIMESTAMP
        ORDER BY
            ACCOUNTBALANCE.VALUE_TIME ASC LIMIT 1
    ) AS ACTUAL_BAL_AMT,
    OFFICER_ID,
    PROFIT_CENTRE,
    NARRATIVE
FROM
    POSTINGINSTRUCTION
    JOIN ACCOUNTEVENTCREATED
    ON POSTINGINSTRUCTION.ACCT_NO = ACCOUNTEVENTCREATED.ACCT_NO




with max_timestamp as (
    select max(value_time) as max_value_time, account_id
    from Posting_Instruction
)
    SELECT
        POSTINGINSTRUCTION.SYM_RUN_DATE,
        POSTINGINSTRUCTION.BRANCH_NO,
        ACCOUNTEVENTCREATED.CLIENT_NO,
        ACCOUNTEVENTCREATED.CLIENT_NAME,
        POSTINGINSTRUCTION.ACCT_NO,
        POSTINGINSTRUCTION.ACCT_TYPE,
        POSTINGINSTRUCTION.ACCT_TYPE_DESC,
        POSTINGINSTRUCTION.CURRENCY,
        POSTINGINSTRUCTION.TRAN_DATE,
        POSTINGINSTRUCTION.TRAN_TYPE,
        POSTINGINSTRUCTION.TRANSACTION_NO,
        POSTINGINSTRUCTION.POST_DATE,
        POSTINGINSTRUCTION.POST_STATUS,
        POSTINGINSTRUCTION.REVERSAL_TRAN_TYPE,
        POSTINGINSTRUCTION.REVERSAL_DATE,
        POSTINGINSTRUCTION.SOURCE_TYPE,
        POSTINGINSTRUCTION.GL_CODE,
        ACCOUNTBALANCEBEFORE.PREVIOUS_BAL_AMT,
        POSTINGINSTRUCTION.DEBIT_CREDIT_IND,
        POSTINGINSTRUCTION.PSNO,
        POSTINGINSTRUCTION.PSCO,
        CASE
            when IS_CREDIT = 'true' then (ACCOUNTBALANCEBEFORE.PREVIOUS_BAL_AMT + value_posting)
            else (ACCOUNTBALANCEBEFORE.PREVIOUS_BAL_AMT - value_posting)
        end as ACTUAL_BAL_AMT,
        POSTINGINSTRUCTION.OFFICER_ID,
        POSTINGINSTRUCTION.PROFIT_CENTRE,
        POSTINGINSTRUCTION.NARRATIVE
    FROM
        POSTINGINSTRUCTION
        JOIN ACCOUNTEVENTCREATED
        ON POSTINGINSTRUCTION.ACCT_NO = ACCOUNTEVENTCREATED.ACCT_NO 
        LEFT JOIN (
            SELECT SODU_NT as PREVIOUS_BAL_AMT, ACCOUNT_ID
            FROM ACCOUNTBALANCE
            JOIN max_timestamp ON AccountBalance.ACCOUNT_ID = max_timestamp.ACCOUNT_ID
            WHERE ACCOUNTBALANCE.VALUE_TIME < max_timestamp.max_value_time
            ORDER BY ACCOUNTBALANCE.VALUE_TIME DESC 
            LIMIT 1
        ) ACCOUNTBALANCEBEFORE ON ACCOUNTBALANCEBEFORE.ACCOUNT_ID = POSTINGINSTRUCTION.ACCT_NO;