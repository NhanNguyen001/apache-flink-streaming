package com.techxcorp.table.vkdepositbal.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.SYNC_DATETIME_PROCCES;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.ACCT_NO;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.ACTUAL_BAL;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.AMOUNT;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.GL_CODE_DUCHI;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.ID;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.LOAI_TIEN;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.NARRATIVE;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.SODU_NT;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.SODU_QD;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.SYM_RUN_DATE;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.TRAN_DATE_TIME;
import static com.techxcorp.table.vkdepositbal.columns.VKDepositBalColumns.VIKKI_TRANSACTION_ID;

public class VKDepositBalSchema {
    public static Schema schema(){
        return  Schema.newBuilder()
                .column(SYM_RUN_DATE, DataTypes.DATE())
                .column(ACCT_NO, DataTypes.STRING())
                .column(SODU_NT, DataTypes.DECIMAL(22, 5))
                .column(SODU_QD, DataTypes.INT())
                .column(TRAN_DATE_TIME, DataTypes.TIMESTAMP(3))
                .column(VIKKI_TRANSACTION_ID, DataTypes.STRING())
                .column(NARRATIVE, DataTypes.STRING())
                .column(AMOUNT, DataTypes.DECIMAL(22, 5))
                .column(ACTUAL_BAL, DataTypes.DECIMAL(22, 5))
                .column(ID, DataTypes.STRING().notNull())
                .column(LOAI_TIEN, DataTypes.STRING())
                .column(GL_CODE_DUCHI, DataTypes.STRING())
                .column(SYNC_DATETIME_PROCCES, DataTypes.TIMESTAMP(3))
                .primaryKey(ID)
                .build();
    }
}
