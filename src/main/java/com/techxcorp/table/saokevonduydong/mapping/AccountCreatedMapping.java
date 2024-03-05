package com.techxcorp.table.saokevonduydong.mapping;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.techxcorp.SerDes.MappingFunctionJson;
import com.techxcorp.table.saokevonduydong.dto.InstanceParamValsDto;
import com.techxcorp.utils.DataUtils;
import com.techxcorp.utils.JsonUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.ACCT_NO;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.CLIENT_NO;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.ID;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.KYHAN_SO;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.LAISUAT;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.LAISUAT_DCV;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.LOAI_KYHAN;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.MA_LOAI;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.NGAY_MOSO;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.PERIOD;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.PERIOD_TYPE;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.SYM_RUN_DATE;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.TEN_CN;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.THI_TRUONG;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.VALUE_TIMESTAMP;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.ACCT_OPEN_DATE;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.ACCT_MATURITY_DATE;
import static com.techxcorp.table.saokevonduydong.columns.AccountCreatedColumns.BRANCH_NO;
import static com.techxcorp.utils.DataUtils.convertPeriodType;
import static com.techxcorp.utils.DataUtils.getCif;
import static com.techxcorp.utils.DataUtils.getTenor;
import static com.techxcorp.utils.JsonUtils.fromJsonArray;
import static com.techxcorp.utils.JsonUtils.getDataString;
import static java.lang.String.format;


public class AccountCreatedMapping implements MappingFunctionJson {

        private final List<String> productIds = List.of("time_deposit", "current_account");

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();

                JsonObject jsonObj = input.getAsJsonObject();
                JsonObject account = jsonObj.getAsJsonObject("account");

                String productId = getDataString(account, "product_id");

                if (filterAccount(productId)) {
                        JsonObject technicalMetadata = jsonObj.getAsJsonObject("technical_metadata");
                        String accountId = getDataString(account, "id");

                        TypeReference<List<String>> listType = new TypeReference<>() {};
                        List<String> clientNo = fromJsonArray(account.get("stakeholder_ids").toString(), listType);

                        String eventId = getDataString(technicalMetadata, "event_id");

                        InstanceParamValsDto dto = JsonUtils.fromJsonElement(account.get("instance_param_vals"), InstanceParamValsDto.class);

                        final Row row = Row.withNames(RowKind.INSERT);

                        String openingTimestamp = getDataString(account, "opening_timestamp");
                        LocalDate symRunDate = LocalDate.parse(openingTimestamp.split("T")[0]);
                        LocalDateTime openingDataTime = LocalDateTime.parse(openingTimestamp, DateTimeFormatter.ISO_DATE_TIME);

                        String depositStartDatetime = dto.getDepositStartDatetime();

                        LocalDate maturityDate = DataUtils.calculateMaturityDateByTenor(DataUtils.calculateEffectiveDateTime(depositStartDatetime), dto.getTenorUnit(), dto.getTenorAmount());

                        row.setField(SYM_RUN_DATE, symRunDate);
                        row.setField(ACCT_NO, accountId);
                        row.setField(PERIOD, dto.getTenorAmount());
                        row.setField(PERIOD_TYPE, convertPeriodType(dto.getTenorUnit()));
                        row.setField(LOAI_KYHAN, getTenor(dto.getTenorAmount(), dto.getTenorUnit()));
                        row.setField(CLIENT_NO, getCif(clientNo));
                        row.setField(VALUE_TIMESTAMP, openingDataTime);
                        row.setField(ID, eventId);
                        row.setField(KYHAN_SO, null);
                        row.setField(LAISUAT_DCV, null);
                        row.setField(MA_LOAI, "HD");
                        row.setField(THI_TRUONG, null);
                        row.setField(TEN_CN, null);
                        row.setField(ACCT_OPEN_DATE, openingDataTime);
                        row.setField(ACCT_MATURITY_DATE, maturityDate);
                        row.setField(BRANCH_NO, "689");
                        row.setField(NGAY_MOSO, symRunDate);

                        BigDecimal interestRate = null;
                        if (dto.getAnnualInterestRate() != null) {
                                interestRate = dto.getAnnualInterestRate().multiply(BigDecimal.valueOf(100)).stripTrailingZeros();
                        }

                        String laiSuat = interestRate != null ? format("%s%s", interestRate, "%") : null;
                        row.setField(LAISUAT, laiSuat);

                        rows.add(row);
                }
                return rows;
        }
        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] {
                                        SYM_RUN_DATE,
                                        KYHAN_SO,
                                        LAISUAT_DCV,
                                        MA_LOAI,
                                        THI_TRUONG,
                                        TEN_CN,
                                        ACCT_NO,
                                        PERIOD,
                                        PERIOD_TYPE,
                                        LOAI_KYHAN,
                                        CLIENT_NO,
                                        VALUE_TIMESTAMP,
                                        ID,
                                        ACCT_OPEN_DATE,
                                        ACCT_MATURITY_DATE,
                                        BRANCH_NO,
                                        LAISUAT,
                                        NGAY_MOSO
                                },
                                new TypeInformation[] {
                                        Types.LOCAL_DATE,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.INT,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.LOCAL_DATE,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE,
                                });
        }

        private boolean filterAccount(String productId) {
            return productIds.contains(productId);
        }
}
