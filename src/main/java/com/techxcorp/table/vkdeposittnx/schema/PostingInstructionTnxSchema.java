package com.techxcorp.table.vkdeposittnx.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CLIENT_NAME;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CLIENT_NO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.OFFICER_ID;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SYM_RUN_DATE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.BRANCH_NO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ACCT_NO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ACCT_TYPE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ACCT_TYPE_DESC;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CURRENCY;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CRD_ACC_ID_ISS;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CRD_NUNBER_NO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANS_ID;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRAN_DATE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.POST_DATE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.POST_STATUS;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRAN_TYPE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANSACTION_NO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.REVERSAL_TRAN_TYPE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.REVERSAL_DATE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SOURCE_TYPE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.GL_CODE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CCY;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANS_AMT;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANS_LCY_AMT;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.DEBIT_CREDIT_IND;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PSNO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PSCO;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PROFIT_CENTRE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.NARRATIVE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CAD_TRANS_TYPE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CAD_AUTH_CODE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CLIENT_ID;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.RTF_TYPE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.STATUS;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PHASE;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.VALUE_TIMESTAMP;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.IS_CREDIT;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.AMOUNT;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ID;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRAN_DATE_TIME;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SENDER_NAME;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.RECEIVER_NAME;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SYNC_DATETIME_PROCCES;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANSFER_AMOUNT;
import static com.techxcorp.table.vkdeposittnx.columns.PostingInstructionTnxColumns.POSTING_ID;

public class PostingInstructionTnxSchema {
    public static Schema schema() {
        return Schema.newBuilder()
                .column(SYM_RUN_DATE, DataTypes.DATE())
                .column(BRANCH_NO, DataTypes.STRING())
                .column(ACCT_NO, DataTypes.STRING())
                .column(ACCT_TYPE, DataTypes.STRING())
                .column(ACCT_TYPE_DESC, DataTypes.STRING())
                .column(CURRENCY, DataTypes.STRING())
                .column(CRD_ACC_ID_ISS, DataTypes.STRING())
                .column(CRD_NUNBER_NO, DataTypes.STRING())
                .column(TRANS_ID, DataTypes.STRING())
                .column(TRAN_DATE, DataTypes.DATE())
                .column(POST_DATE, DataTypes.DATE())
                .column(POST_STATUS, DataTypes.STRING())
                .column(TRAN_TYPE, DataTypes.STRING())
                .column(TRANSACTION_NO, DataTypes.STRING())
                .column(REVERSAL_TRAN_TYPE, DataTypes.STRING())
                .column(REVERSAL_DATE, DataTypes.TIMESTAMP(3))
                .column(SOURCE_TYPE, DataTypes.STRING())
                .column(GL_CODE, DataTypes.STRING())
                .column(CCY, DataTypes.STRING())
                .column(TRANS_AMT, DataTypes.DECIMAL(22, 5))
                .column(TRANS_LCY_AMT, DataTypes.DECIMAL(22, 5))
                .column(DEBIT_CREDIT_IND, DataTypes.STRING())
                .column(PSNO, DataTypes.DECIMAL(22, 5))
                .column(PSCO, DataTypes.DECIMAL(22, 5))
                .column(PROFIT_CENTRE, DataTypes.STRING())
                .column(NARRATIVE, DataTypes.STRING())
                .column(CAD_TRANS_TYPE, DataTypes.STRING())
                .column(CAD_AUTH_CODE, DataTypes.STRING())
                .column(CLIENT_ID, DataTypes.STRING())
                .column(RTF_TYPE, DataTypes.STRING())
                .column(STATUS, DataTypes.STRING())
                .column(PHASE, DataTypes.STRING())
                .column(VALUE_TIMESTAMP, DataTypes.TIMESTAMP(3))
                .column(IS_CREDIT, DataTypes.STRING())
                .column(AMOUNT, DataTypes.DECIMAL(22, 5))
                .column(ID, DataTypes.STRING().notNull())
                .column(TRAN_DATE_TIME, DataTypes.TIMESTAMP(3))
                .column(SENDER_NAME, DataTypes.STRING())
                .column(RECEIVER_NAME, DataTypes.STRING())
                .column(SYNC_DATETIME_PROCCES, DataTypes.TIMESTAMP(3))
                .column(TRANSFER_AMOUNT, DataTypes.DECIMAL(22, 5))
                .column(POSTING_ID, DataTypes.STRING())
                .column(CLIENT_NO, DataTypes.STRING())
                .column(CLIENT_NAME, DataTypes.STRING())
                .column(OFFICER_ID, DataTypes.STRING())
                .watermark(VALUE_TIMESTAMP, "VALUE_TIMESTAMP - INTERVAL '1' SECOND")
                .primaryKey(ID)
                .build();
    }
}
