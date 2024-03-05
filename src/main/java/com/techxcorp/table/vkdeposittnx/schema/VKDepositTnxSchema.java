package com.nhannt22.table.vkdeposittnx.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.BRANCH_NO;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.CLIENT_NAME;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.CLIENT_NO;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.SYM_RUN_DATE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.ACCT_NO;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.ACCT_TYPE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.ACCT_TYPE_DESC;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.CURRENCY;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.TRAN_DATE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.TRAN_DATE_TIME;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.TRAN_TYPE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.TRANSACTION_NO;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.POST_DATE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.POST_STATUS;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.REVERSAL_TRAN_TYPE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.REVERSAL_DATE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.SOURCE_TYPE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.GL_CODE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.PREVIOUS_BAL_AMT;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.DEBIT_CREDIT_IND;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.PSNO;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.PSCO;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.ACTUAL_BAL_AMT;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.OFFICER_ID;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.PROFIT_CENTRE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.NARRATIVE;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.POSTING_ID;
import static com.nhannt22.table.vkdeposittnx.columns.VKDepositTnxColumns.SYNC_DATETIME_PROCCES;

public class VKDepositTnxSchema {
    public static Schema schema(){
        return  Schema.newBuilder()
                .column(SYM_RUN_DATE, DataTypes.DATE())
                .column(BRANCH_NO, DataTypes.STRING())
                .column(CLIENT_NO, DataTypes.STRING())
                .column(CLIENT_NAME, DataTypes.STRING())
                .column(ACCT_NO, DataTypes.STRING().notNull())
                .column(ACCT_TYPE, DataTypes.STRING())
                .column(ACCT_TYPE_DESC, DataTypes.STRING())
                .column(CURRENCY, DataTypes.STRING())
                .column(TRAN_DATE, DataTypes.DATE())
                .column(TRAN_DATE_TIME, DataTypes.TIMESTAMP(3))
                .column(TRAN_TYPE, DataTypes.STRING())
                .column(TRANSACTION_NO, DataTypes.STRING())
                .column(POST_DATE, DataTypes.DATE())
                .column(POST_STATUS, DataTypes.STRING())
                .column(REVERSAL_TRAN_TYPE, DataTypes.STRING())
                .column(REVERSAL_DATE, DataTypes.DATE())
                .column(SOURCE_TYPE, DataTypes.STRING())
                .column(GL_CODE, DataTypes.STRING())
                .column(PREVIOUS_BAL_AMT, DataTypes.DECIMAL(26, 8))
                .column(DEBIT_CREDIT_IND, DataTypes.STRING())
                .column(PSNO, DataTypes.DECIMAL(26, 8))
                .column(PSCO, DataTypes.DECIMAL(26, 8))
                .column(ACTUAL_BAL_AMT, DataTypes.DECIMAL(26, 8))
                .column(OFFICER_ID, DataTypes.STRING())
                .column(PROFIT_CENTRE, DataTypes.STRING())
                .column(NARRATIVE, DataTypes.STRING())
                .column(POSTING_ID, DataTypes.STRING().notNull())
                .column(SYNC_DATETIME_PROCCES, DataTypes.TIMESTAMP(3))
                .primaryKey(POSTING_ID)
                .build();
    }
}
