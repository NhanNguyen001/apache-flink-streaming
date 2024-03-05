package com.techxcorp.table.vkdeposittnx.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.SYM_RUN_DATE;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.SODU_NT;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.SODU_QD;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.ACCOUNT_ID;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.VALUE_TIMESTAMP;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.ID;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.PHASE;

public class AccountBalanceTnxSchema {
    public static Schema schema() {
        return Schema.newBuilder()
                .column(SYM_RUN_DATE, DataTypes.DATE())
                .column(SODU_NT, DataTypes.DECIMAL(22, 5))
                .column(SODU_QD, DataTypes.DECIMAL(22, 5))
                .column(ACCOUNT_ID, DataTypes.STRING())
                .column(VALUE_TIMESTAMP, DataTypes.TIMESTAMP(3))
                .column(ID, DataTypes.STRING().notNull())
                .column(PHASE, DataTypes.STRING())
                .watermark(VALUE_TIMESTAMP, "VALUE_TIMESTAMP - INTERVAL '1' SECOND")
                .primaryKey(ID)
                .build();
    };
}
