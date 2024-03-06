package com.nhannt22.mapping;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.nhannt22.SerDes.MappingFunctionJson;

public class VKAccountBalanceMapping implements MappingFunctionJson {
        // raw_tm.vault.core_api.v1.balances.account_balance.events

        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("SYM_RUN_DATE", DataTypes.DATE())
                                .column("SODU_NT", DataTypes.DECIMAL(22, 5))
                                .column("SODU_QD", DataTypes.DECIMAL(22, 5))
                                .column("ACCOUNT_ID", DataTypes.STRING())
                                .column("VALUE_TIMESTAMP", DataTypes.TIMESTAMP(3))
                                .column("ID", DataTypes.STRING().notNull())
                                .column("PHASE", DataTypes.STRING())
                                .column("FILTER_TNX", DataTypes.STRING())
                                .watermark("VALUE_TIMESTAMP", "VALUE_TIMESTAMP - INTERVAL '10' SECONDS")
                                // .columnByExpression("NEW_TIME", "PROCTIME()")
                                .primaryKey("ID")
                                .build();
        };

        public String getDataString(JsonObject jsonObject, String fieldName) {
                return jsonObject.has(fieldName)
                                ? jsonObject.get(fieldName).getAsString()
                                : "";
        }

        public BigDecimal getDataNumber(JsonObject jsonObject, String fieldName) {
                return jsonObject.has(fieldName)
                                ? jsonObject.get(fieldName).getAsBigDecimal()
                                : BigDecimal.ZERO;
        }

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();
                JsonObject jsonObj = input.getAsJsonObject();
                JsonArray balances = jsonObj.getAsJsonArray("balances");
                String postingId = "";
                for (int i = 0; i < balances.size(); i++) {
                        final Row row = Row.withNames(RowKind.INSERT);
                        row.setField("SYM_RUN_DATE",
                                        LocalDate.parse(getDataString(jsonObj, "timestamp").split("T")[0]));
                        row.setField("SODU_NT", balances.get(i).getAsJsonObject().get("amount").getAsBigDecimal());
                        row.setField("SODU_QD", balances.get(i).getAsJsonObject().get("amount").getAsBigDecimal());
                        String accountId = getDataString(jsonObj, "account_id");
                        row.setField("ACCOUNT_ID", accountId);
                        // String postingInstructionBatchId = getDataString(jsonObj,
                        // "posting_instruction_batch_id");
                        String postingInstructionBatchId = getDataString(balances.get(i).getAsJsonObject(),
                                        "posting_instruction_batch_id");
                        String valueTime = getDataString(jsonObj, "timestamp");

                        row.setField("VALUE_TIMESTAMP",
                                        LocalDateTime.parse(valueTime, DateTimeFormatter.ISO_DATE_TIME));
                        row.setField("ID", postingInstructionBatchId + accountId);
                        String phase  = getDataString(balances.get(i).getAsJsonObject(), "phase");
                        row.setField("PHASE", phase);
                        String filter_condition = "0";
                        if(postingInstructionBatchId.length() > 0 && phase.equals("POSTING_PHASE_COMMITTED") ) {
                                filter_condition = "1";
                        }
                        row.setField("FILTER_TNX", filter_condition);
                        rows.add(row);
                        postingId = postingInstructionBatchId;
                }

                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "SYM_RUN_DATE", "SODU_NT", "SODU_QD", "ACCOUNT_ID", "VALUE_TIMESTAMP",
                                                "ID",
                                                "PHASE", "FILTER_TNX"
                                },
                                new TypeInformation[] { Types.LOCAL_DATE, Types.BIG_DEC, Types.BIG_DEC, Types.STRING,
                                                Types.LOCAL_DATE_TIME, Types.STRING, Types.STRING, Types.STRING });
        }
}
