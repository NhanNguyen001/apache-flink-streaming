package com.nhannt22.mapping;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
// import java.util.UUID;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.nhannt22.SerDes.MappingFunctionJson;

public class VKClientMapping implements MappingFunctionJson {
        // raw_party.sit.party_service.create
        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("SYM_RUN_DATE", DataTypes.DATE())
                                .column("CLIENT_NO", DataTypes.STRING().notNull())
                                .column("CLIENT_NAME", DataTypes.STRING())
                                .column("CLIENT_SHORT", DataTypes.STRING())
                                .column("CLIENT_STATUS", DataTypes.STRING())
                                .column("REGISTERED_DATE", DataTypes.DATE())
                                .column("DT_OF_ISSUANCE", DataTypes.DATE())
                                .column("TRAN_STATUS", DataTypes.STRING())
                                .column("PHONE", DataTypes.STRING())
                                .column("BIRTH_DATE", DataTypes.DATE())
                                .column("GLOBAL_ID", DataTypes.STRING())
                                .column("SYNC_DATETIME_PROCCES", DataTypes.TIMESTAMP(3))
                                .column("CLIENT_NAME_VN", DataTypes.STRING())
                                .primaryKey("CLIENT_NO")
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
                JsonObject details = jsonObj.get("details").getAsJsonObject();

                final Row row = Row.withNames(RowKind.INSERT);
                try {
                        // get system date
                        row.setField("SYM_RUN_DATE",
                                        LocalDate.parse(getDataString(jsonObj, "eventTime").split("T")[0]));

                        row.setField("CLIENT_NO", details.get("cifNumber").getAsString());
                        row.setField("CLIENT_NAME", details.get("fullNameEn").getAsString());
                        row.setField("CLIENT_NAME_VN", details.get("fullNameVn").getAsString());
                        row.setField("CLIENT_SHORT", details.get("fullNameEn").getAsString());

                        row.setField("CLIENT_STATUS", details.get("status").getAsString());
                        row.setField("REGISTERED_DATE",
                                        LocalDate.parse(details.get("onboardingTimestamp").getAsString()
                                                        .split("\\s")[0]));
                        row.setField("DT_OF_ISSUANCE", LocalDate
                                        .parse(details.getAsJsonObject("nationalId").get("issueDate").getAsString()));
                        row.setField("TRAN_STATUS", details.get("status").getAsString());
                        row.setField("PHONE", details.get("onboardPhoneNumber").getAsString());
                        row.setField("BIRTH_DATE",
                                        LocalDate.parse(getDataString(details, "birthdate")));
                        row.setField("GLOBAL_ID", getDataString(details.get("nationalId").getAsJsonObject(), "number"));
                        row.setField("SYNC_DATETIME_PROCCES", LocalDateTime.now());
                        rows.add(row);
                } catch (Exception e) {
                        System.out.println(e);
                }
                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "SYM_RUN_DATE", "CLIENT_NO", "CLIENT_NAME", "CLIENT_SHORT",
                                                "CLIENT_STATUS", "REGISTERED_DATE", "DT_OF_ISSUANCE",
                                                "TRAN_STATUS",
                                                "PHONE", "BIRTH_DATE", "GLOBAL_ID", "SYNC_DATETIME_PROCCES",
                                                "CLIENT_NAME_VN" },
                                new TypeInformation[] { Types.LOCAL_DATE, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING, Types.LOCAL_DATE, Types.LOCAL_DATE,
                                                Types.STRING,
                                                Types.STRING, Types.LOCAL_DATE, Types.STRING, Types.LOCAL_DATE_TIME,
                                                Types.STRING });
        }
}
