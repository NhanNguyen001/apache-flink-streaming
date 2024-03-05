package com.nhannt22.mapping;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

public class VKCardDimMapping implements MappingFunctionJson {
        // raw_party.sit.party_service.create
        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("SYM_RUN_DATE", DataTypes.DATE())
                                .column("CLIENT_NO", DataTypes.STRING())
                                .column("CLIENT_NAME", DataTypes.STRING())
                                .column("CRD_CONTRACT_ID", DataTypes.STRING())
                                .column("CRD_CONTRACT_OPEN_DATE", DataTypes.TIMESTAMP(3))
                                .column("CRD_CONTRACT_STATUS", DataTypes.STRING())
                                .column("CRD_ACC_ID_ISS", DataTypes.STRING())
                                .column("CRD_TARGET_ID", DataTypes.STRING().notNull())
                                .column("CRD_NUNBER_NO", DataTypes.STRING())
                                .column("CRD_ACC_NO", DataTypes.STRING())
                                .column("CRD_ACC_STATUS_ISS", DataTypes.STRING())
                                .column("CRD_ACC_STATUS_DATE_ISS", DataTypes.TIMESTAMP(3))
                                .column("CRD_ACC_OPEN_DATE", DataTypes.STRING())
                                .column("CRD_ACC_EXPIRE_DATE", DataTypes.STRING())
                                .column("CRD_ACC_UNLOCK_DATE", DataTypes.TIMESTAMP(3))
                                .column("CRD_ACC_DESTR", DataTypes.TIMESTAMP(3))
                                .column("CRD_ACC_STATUS", DataTypes.STRING())
                                .column("CRD_ACC_STATUS_DATE", DataTypes.STRING())
                                .column("CRD_ACC_BRAND", DataTypes.STRING())
                                .column("CRD_ACC_LEVEL", DataTypes.STRING())
                                .column("CRD_AUTH_AMT_CONTRACT", DataTypes.DECIMAL(22, 5))
                                .column("CRD_AUTH_LCY_AMT_CONTRACT", DataTypes.DECIMAL(22, 5))
                                .column("CRD_AUTH_AMT", DataTypes.DECIMAL(22, 5))
                                .column("CRD_AUTH_LCY_AMT", DataTypes.DECIMAL(22, 5))
                                .column("CRD_GROUP", DataTypes.STRING())
                                .column("CRD_MAIN", DataTypes.STRING())
                                .column("CRD_REPLACE_ID", DataTypes.STRING())
                                .column("CRD_REPLACE_REASON", DataTypes.STRING())
                                .column("CRD_REPLACE_DATE", DataTypes.TIMESTAMP(3))
                                .column("GL_CODE", DataTypes.STRING())
                                .column("CRD_ACCOUNT_CASA_ID", DataTypes.STRING())
                                .column("FLAG_CRD_ACCOUNT_AUTO", DataTypes.STRING())
                                .column("CRD_RATE", DataTypes.DECIMAL(22, 5))
                                .column("CRD_OUSTANDING_AMT", DataTypes.DECIMAL(22, 5))
                                .column("CRD_OUSTANDING_LCY_AMT", DataTypes.DECIMAL(22, 5))
                                .column("SYNC_DATETIME_PROCCES", DataTypes.TIMESTAMP(3))
                                .primaryKey("CRD_TARGET_ID")
                                .build();
        };

        public String getDataString(JsonObject jsonObject, String fieldName) {
                return jsonObject.has(fieldName) && !jsonObject.get(fieldName).isJsonNull()
                                ? jsonObject.get(fieldName).getAsString()
                                : "";
        }

        public BigDecimal getDataNumber(JsonObject jsonObject, String fieldName) {
                return jsonObject.has(fieldName) && !jsonObject.get(fieldName).isJsonNull()
                                ? jsonObject.get(fieldName).getAsBigDecimal()
                                : null;
        }
        

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();
                JsonObject jsonObj = input.getAsJsonObject();
                JsonObject request = jsonObj.get("request").getAsJsonObject();
                JsonObject response = jsonObj.get("response").getAsJsonObject();

                final Row row = Row.withNames(RowKind.INSERT);
                try {
                        Long createdDateTime = jsonObj.get("createdDateTime").getAsLong();
                        LocalDateTime time = Instant.ofEpochMilli(createdDateTime)
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDateTime();
                        LocalDate date = Instant.ofEpochMilli(createdDateTime)
                                        .atZone(ZoneId.systemDefault())
                                        .toLocalDate();
                        row.setField("SYM_RUN_DATE", date);
                        row.setField("CLIENT_NO", getDataString(jsonObj, "cif"));
                        String firstName = getDataString(request, "firstName");
                        String lastName = getDataString(request, "lastName");
                        String fullname = firstName + " " + lastName;
                        fullname = fullname.trim();
                        String eventType = getDataString(jsonObj, "eventType");
                        row.setField("CLIENT_NAME", fullname);
                        row.setField("CRD_CONTRACT_ID", getDataString(response, "onboardingId"));
                        row.setField("CRD_CONTRACT_OPEN_DATE", time);
                        // if eventSubType == 'CARD_UNLOCK': ACTIVE
                        // if eventSubType == 'CARD_LOCK' : BLOCK
                        // if eventSubType == 'Thay đổi hạn mức' , 'RENEW' thẻ hết hạn - cấp thẻ mới
                        String eventSubType = getDataString(jsonObj, "eventSubType");
                        if (eventSubType.equalsIgnoreCase("CARD_UNLOCK")) {
                                row.setField("CRD_CONTRACT_STATUS", "ACTIVE");
                                row.setField("CRD_ACC_STATUS_ISS", "ACTIVE");
                                row.setField("CRD_ACC_STATUS", "ACTIVE");
                        } else if (eventSubType.equalsIgnoreCase("CARD_LOCK")) {
                                row.setField("CRD_CONTRACT_STATUS", "BLOCK");
                                row.setField("CRD_ACC_STATUS_ISS", "BLOCK");
                                row.setField("CRD_ACC_STATUS", "BLOCK");
                        } else if (eventSubType.equalsIgnoreCase("Thay đổi hạn mức")) {
                                row.setField("CRD_CONTRACT_STATUS", "RENEW");
                                row.setField("CRD_ACC_STATUS_ISS", "RENEW");
                                row.setField("CRD_ACC_STATUS", "RENEW");
                        }
                        row.setField("CRD_ACC_ID_ISS", getDataString(response, "contractNumber"));

                        if(eventType.equalsIgnoreCase("CARD_CREATION")) {
                                row.setField("CRD_TARGET_ID", getDataString(response, "cardId"));
                        } else{
                                row.setField("CRD_TARGET_ID", getDataString(request, "cardId"));
                        }
                        row.setField("CRD_NUNBER_NO", getDataString(response, "cardNumber"));
                        row.setField("CRD_ACC_NO", getDataString(response, "cardNumber"));
                        row.setField("CRD_ACC_NO", getDataString(request, "cbsNumber1"));
                        
                        if (eventType.equalsIgnoreCase("CARD_MODIFICATION")) {
                                row.setField("CRD_ACC_STATUS_DATE_ISS", time);
                                row.setField("CRD_ACC_STATUS_DATE", createdDateTime.toString());
                                if (eventSubType.equalsIgnoreCase("CARD_UNLOCK")) {
                                        row.setField("CRD_ACC_UNLOCK_DATE", time);
                                }
                        }
                        row.setField("CRD_ACC_OPEN_DATE", createdDateTime.toString());
                        row.setField("CRD_ACC_EXPIRE_DATE", getDataString(response, "expirityDate"));
                        row.setField("CRD_ACCOUNT_CASA_ID", getDataString(request, "cbsNumber1"));
                        
                        row.setField("SYNC_DATETIME_PROCCES", LocalDateTime.now());

                        rows.add(row);
                } catch (Exception e) {
                        System.out.println(e);
                        System.out.println(jsonObj);
                }
                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "SYM_RUN_DATE", "CLIENT_NO", "CLIENT_NAME", "CRD_CONTRACT_ID",
                                                "CRD_CONTRACT_OPEN_DATE", "CRD_CONTRACT_STATUS", "CRD_ACC_ID_ISS",
                                                "CRD_TARGET_ID",
                                                "CRD_NUNBER_NO", "CRD_ACC_NO", "CRD_ACC_STATUS_ISS",
                                                "CRD_ACC_STATUS_DATE_ISS",
                                                "CRD_ACC_OPEN_DATE", "CRD_ACC_EXPIRE_DATE", "CRD_ACC_UNLOCK_DATE",
                                                "CRD_ACC_DESTR",
                                                "CRD_ACC_STATUS", "CRD_ACC_STATUS_DATE", "CRD_ACC_BRAND",
                                                "CRD_ACC_LEVEL",
                                                "CRD_AUTH_AMT_CONTRACT", "CRD_AUTH_LCY_AMT_CONTRACT", "CRD_AUTH_AMT",
                                                "CRD_AUTH_LCY_AMT", "CRD_GROUP", "CRD_MAIN", "CRD_REPLACE_ID",
                                                "CRD_REPLACE_REASON",
                                                "CRD_REPLACE_DATE", "GL_CODE", "CRD_ACCOUNT_CASA_ID",
                                                "FLAG_CRD_ACCOUNT_AUTO",
                                                "CRD_RATE", "CRD_OUSTANDING_AMT", "CRD_OUSTANDING_LCY_AMT", "SYNC_DATETIME_PROCCES"
                                },
                                new TypeInformation[] { Types.LOCAL_DATE, Types.STRING, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE_TIME, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING, Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME,
                                                Types.STRING, Types.STRING, Types.LOCAL_DATE_TIME,
                                                Types.LOCAL_DATE_TIME, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING, Types.BIG_DEC, Types.BIG_DEC, Types.BIG_DEC,
                                                Types.BIG_DEC, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE_TIME, Types.STRING, Types.STRING, Types.STRING,
                                                Types.BIG_DEC, Types.BIG_DEC, Types.BIG_DEC, Types.LOCAL_DATE_TIME
                                });
        }
}
