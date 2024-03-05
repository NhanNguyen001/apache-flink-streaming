package com.techxcorp.mapping;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
// import java.util.UUID;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.techxcorp.SerDes.MappingFunctionJson;

public class VKAccountEventsMapping implements MappingFunctionJson {
        // raw_tm.vault.core_api.v1.accounts.account.events
        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("EVENT_INFO", DataTypes.STRING())
                                .column("CLIENT_NO", DataTypes.STRING())
                                .column("CLIENT_NAME", DataTypes.STRING())
                                .column("ACCT_NO", DataTypes.STRING().notNull())
                                .column("PRODUCT_ID", DataTypes.STRING())
                                .column("PROC_TIME", DataTypes.TIMESTAMP(3))
                                .watermark("PROC_TIME", "PROC_TIME - INTERVAL '10' MINUTES")
                                .primaryKey("ACCT_NO")
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
                // get account_created or account_updated
                JsonObject eventType;
                String eventInfo = "";
                if (jsonObj.has("account_created")) {
                        eventType = jsonObj.get("account_created").getAsJsonObject();
                        eventInfo = "account_created";
                } else {
                        eventType = jsonObj.get("account_updated").getAsJsonObject();
                        eventInfo = "account_updated";
                }

                final Row row = Row.withNames(RowKind.INSERT);

                JsonArray stakeholderIds = eventType.getAsJsonObject("metadata/account").getAsJsonArray("stakeholder_ids");
                String CLIENT_NO = "";
                if (stakeholderIds.size() >= 1) {
                        CLIENT_NO = eventType.getAsJsonObject("metadata/account").getAsJsonArray("stakeholder_ids").get(0)
                                        .getAsString();
                }
                // get system date
                row.setField("EVENT_INFO", eventInfo);
                row.setField("CLIENT_NO", CLIENT_NO);
                row.setField("CLIENT_NAME", eventType.getAsJsonObject("metadata/account").get("name").getAsString());
                row.setField("ACCT_NO", eventType.getAsJsonObject("metadata/account").get("id").getAsString());
                row.setField("PRODUCT_ID",
                                getDataString(eventType.get("metadata/account").getAsJsonObject(), "product_version_id"));

                String value_timestamp = getDataString(jsonObj,
                                "timestamp");
                LocalDateTime valueTimestamp = LocalDateTime.parse(value_timestamp,
                                DateTimeFormatter.ISO_DATE_TIME);
                ZonedDateTime zdt = ZonedDateTime.of(valueTimestamp,
                                java.time.ZoneId.of("UTC"));
                long epoch = zdt.toInstant().toEpochMilli();
                row.setField("PROC_TIME", valueTimestamp);
                rows.add(row);

                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "EVENT_INFO", "CLIENT_NO", "CLIENT_NAME", "ACCT_NO", "PRODUCT_ID",
                                                "PROC_TIME" },
                                new TypeInformation[] { Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING, Types.LOCAL_DATE_TIME });
        }
}
