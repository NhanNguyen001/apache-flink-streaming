package com.nhannt22.table.vkdepositbal.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.nhannt22.table.vkdepositbal.columns.AccountBalanceColumns.ID;
import static com.nhannt22.table.vkdepositbal.columns.AccountBalanceColumns.SODU_NT;
import static com.nhannt22.table.vkdepositbal.columns.AccountBalanceColumns.SODU_QD;
import static com.nhannt22.table.vkdepositbal.columns.AccountBalanceColumns.VALUE_TIMESTAMP;

public class AccountBalanceSchema {
    public static Schema schema() {
        return Schema.newBuilder()
                .column(ID, DataTypes.STRING().notNull())
                .column(SODU_NT, DataTypes.DECIMAL(22, 5))
                .column(SODU_QD, DataTypes.INT())
                .column(VALUE_TIMESTAMP, DataTypes.TIMESTAMP(3))
                .watermark(VALUE_TIMESTAMP, "VALUE_TIMESTAMP - INTERVAL '1' SECOND")
                .primaryKey(ID)
                .build();
    };
}
