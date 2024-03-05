package com.techxcorp.table.vkdeposittnx.mapping;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.techxcorp.SerDes.MappingFunctionJson;
import com.techxcorp.table.vkdeposittnx.dto.AccountBalanceTnxDto;
import com.techxcorp.utils.DataUtils;
import com.techxcorp.utils.JsonUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.SYM_RUN_DATE;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.SODU_NT;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.SODU_QD;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.ACCOUNT_ID;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.VALUE_TIMESTAMP;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.ID;
import static com.techxcorp.table.vkdeposittnx.columns.AccountBalanceTnxColumns.PHASE;
import static com.techxcorp.utils.DataUtils.getDataStreamValid;
import static com.techxcorp.utils.DataUtils.isDataValid;
import static com.techxcorp.utils.JsonUtils.getDataString;

public class AccountBalanceVkDepositTnxMapping implements MappingFunctionJson {

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();

                JsonObject jsonObj = input.getAsJsonObject();
                String valueTime = getDataString(jsonObj, "timestamp");
                JsonArray balances = jsonObj.getAsJsonArray("balances");

                for (int i = 0; i < balances.size(); i++) {
                        final Row row = Row.withNames(RowKind.INSERT);

                        AccountBalanceTnxDto dto = JsonUtils.fromJsonElement(balances.get(i), AccountBalanceTnxDto.class);
                        DataUtils.DataStreamValid streamData = getDataStreamValid(dto.getAccountId(), dto.getPhase(), dto.getAccountAddress());
                        if (isDataValid(streamData)) {
                                BigDecimal amount = dto.getAmount();
                                LocalDate symRunDate = LocalDate.parse(valueTime.split("T")[0]);

                                row.setField(SYM_RUN_DATE,symRunDate);
                                row.setField(SODU_NT,amount);
                                row.setField(SODU_QD,amount);
                                row.setField(ACCOUNT_ID,dto.getAccountId());
                                row.setField(VALUE_TIMESTAMP,LocalDateTime.parse(valueTime, DateTimeFormatter.ISO_DATE_TIME));
                                row.setField(ID,dto.getPostingInstructionBatchId() + dto.getAccountId());
                                row.setField(PHASE,dto.getPhase());
                                rows.add(row);
                        }


                }
                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] {
                                        SYM_RUN_DATE,
                                        SODU_NT,
                                        SODU_QD,
                                        ACCOUNT_ID,
                                        VALUE_TIMESTAMP,
                                        ID,
                                        PHASE,
                                },
                                new TypeInformation[] {
                                        Types.LOCAL_DATE,
                                        Types.BIG_DEC,
                                        Types.BIG_DEC,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.STRING,
                                        Types.STRING,
                                });
        }
}
