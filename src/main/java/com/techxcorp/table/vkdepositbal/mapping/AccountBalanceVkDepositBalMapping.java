package com.techxcorp.table.vkdepositbal.mapping;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.techxcorp.SerDes.MappingFunctionJson;
import com.techxcorp.table.vkdepositbal.dto.AccountBalanceDto;
import com.techxcorp.utils.DataUtils;
import com.techxcorp.utils.JsonUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static com.techxcorp.table.vkdepositbal.columns.AccountBalanceColumns.ID;
import static com.techxcorp.table.vkdepositbal.columns.AccountBalanceColumns.SODU_NT;
import static com.techxcorp.table.vkdepositbal.columns.AccountBalanceColumns.SODU_QD;
import static com.techxcorp.table.vkdepositbal.columns.AccountBalanceColumns.VALUE_TIMESTAMP;
import static com.techxcorp.utils.DataUtils.getDataStreamValid;
import static com.techxcorp.utils.DataUtils.isDataValid;
import static com.techxcorp.utils.JsonUtils.getDataString;

public class AccountBalanceVkDepositBalMapping implements MappingFunctionJson {

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();

                JsonObject jsonObj = input.getAsJsonObject();
                String valueTime = getDataString(jsonObj, "timestamp");
                JsonArray balances = jsonObj.getAsJsonArray("balances");

                for (int i = 0; i < balances.size(); i++) {
                        final Row row = Row.withNames(RowKind.INSERT);

                        AccountBalanceDto dto = JsonUtils.fromJsonElement(balances.get(i), AccountBalanceDto.class);
                        DataUtils.DataStreamValid streamData = getDataStreamValid(dto.getAccountId(), dto.getPhase(), dto.getAccountAddress());
                        if (isDataValid(streamData)) {
                                row.setField(ID, dto.getPostingInstructionBatchId() + dto.getAccountId());
                                BigDecimal amount = dto.getAmount();
                                row.setField(SODU_NT, amount);
                                row.setField(SODU_QD, amount.intValue());
                                row.setField(VALUE_TIMESTAMP, LocalDateTime.parse(valueTime, DateTimeFormatter.ISO_DATE_TIME));

                                rows.add(row);
                        }


                }
                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] {
                                        ID,
                                        SODU_NT,
                                        SODU_QD,
                                        VALUE_TIMESTAMP
                                },
                                new TypeInformation[] {
                                        Types.STRING,
                                        Types.BIG_DEC,
                                        Types.INT,
                                        Types.LOCAL_DATE_TIME
                                });
        }
}
