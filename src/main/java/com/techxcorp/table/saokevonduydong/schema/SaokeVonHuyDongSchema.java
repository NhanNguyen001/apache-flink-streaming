package com.techxcorp.table.saokevonduydong.schema;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.NGAY_MOSO;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.ACCT_NO;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.CLIENT_NO;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.KYHAN_SO;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.LAISUAT;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.LAISUAT_DCV;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.LOAI_KYHAN;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.MA_LOAI;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.PERIOD;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.PERIOD_TYPE;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.SYM_RUN_DATE;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.SYNC_DATETIME_PROCCES;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.TEN_CN;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.THI_TRUONG;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.ID;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.ACCT_OPEN_DATE;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.ACCT_MATURITY_DATE;
import static com.techxcorp.table.saokevonduydong.columns.SaokeVonhuydongColumns.BRANCH_NO;


public class SaokeVonHuyDongSchema {
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
                .column(ID, DataTypes.STRING().notNull())
                .column(SYNC_DATETIME_PROCCES, DataTypes.TIMESTAMP(3))
                .column(ACCT_OPEN_DATE, DataTypes.TIMESTAMP(3))
                .column(ACCT_MATURITY_DATE, DataTypes.DATE())
                .column(BRANCH_NO, DataTypes.STRING())
                .column(LAISUAT, DataTypes.STRING())
                .column(NGAY_MOSO, DataTypes.DATE())
                .primaryKey(ID)
                .build();
    }
}
