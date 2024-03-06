package com.nhannt22.table.saokevonduydong.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.ACCT_NO;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.CLIENT_NO;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.LAISUAT;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.LOAI_KYHAN;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.NGAY_MOSO;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.PERIOD;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.PERIOD_TYPE;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.SYM_RUN_DATE;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.KYHAN_SO;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.LAISUAT_DCV;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.MA_LOAI;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.TEN_CN;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.THI_TRUONG;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.VALUE_TIMESTAMP;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.ID;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.ACCT_OPEN_DATE;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.ACCT_MATURITY_DATE;
import static com.nhannt22.table.saokevonduydong.columns.AccountCreatedColumns.BRANCH_NO;


public class AccountCreatedSchema {
    public static Schema schema() {
        return Schema.newBuilder()
                .column(SYM_RUN_DATE, DataTypes.DATE())
                .column(MA_LOAI, DataTypes.STRING())
                .column(TEN_CN, DataTypes.STRING())
                .column(THI_TRUONG, DataTypes.STRING())
                .column(KYHAN_SO, DataTypes.STRING())
                .column(LAISUAT_DCV, DataTypes.STRING())
                .column(ACCT_NO, DataTypes.STRING())
                .column(PERIOD, DataTypes.INT())
                .column(PERIOD_TYPE, DataTypes.STRING())
                .column(LOAI_KYHAN, DataTypes.STRING())
                .column(CLIENT_NO, DataTypes.STRING())
                .column(VALUE_TIMESTAMP, DataTypes.TIMESTAMP(3))
                .column(ID, DataTypes.STRING().notNull())
                .column(ACCT_OPEN_DATE, DataTypes.TIMESTAMP(3))
                .column(ACCT_MATURITY_DATE, DataTypes.DATE())
                .column(BRANCH_NO, DataTypes.STRING())
                .column(LAISUAT, DataTypes.STRING())
                .column(NGAY_MOSO, DataTypes.DATE())
                .watermark(VALUE_TIMESTAMP, "VALUE_TIMESTAMP - INTERVAL '1' SECOND")
                .primaryKey(ID)
                .build();
    };
}
