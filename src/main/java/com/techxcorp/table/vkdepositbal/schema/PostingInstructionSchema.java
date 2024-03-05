package com.nhannt22.table.vkdepositbal.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.ACCT_NO;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.AMOUNT;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.GL_CODE_DUCHI;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.ID;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.LOAI_TIEN;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.NARRATIVE;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.SYM_RUN_DATE;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.TRANS_ID;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.TRAN_DATE_TIME;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.VALUE_TIMESTAMP;

public class PostingInstructionSchema {
    public static Schema schema() {
        return Schema.newBuilder()
                .column(SYM_RUN_DATE, DataTypes.DATE())
                .column(ACCT_NO, DataTypes.STRING())
                .column(NARRATIVE, DataTypes.STRING())
                .column(AMOUNT, DataTypes.DECIMAL(22, 5))
                .column(LOAI_TIEN, DataTypes.STRING())
                .column(TRANS_ID, DataTypes.STRING())
                .column(GL_CODE_DUCHI, DataTypes.STRING())
                .column(TRAN_DATE_TIME, DataTypes.TIMESTAMP(3))
                .column(VALUE_TIMESTAMP, DataTypes.TIMESTAMP(3))
                .column(ID, DataTypes.STRING().notNull())
                .watermark(VALUE_TIMESTAMP, "VALUE_TIMESTAMP - INTERVAL '1' SECOND")
                .primaryKey(ID)
                .build();
    }
}
