package com.techxcorp.mapping;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
import com.techxcorp.SerDes.MappingFunctionJson;

public class VKCardTnxMapping implements MappingFunctionJson {
        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("CRD_ACC_ID_ISS", DataTypes.STRING())
                                .column("CRD_TARGET_ID", DataTypes.STRING())
                                .column("CRD_NUNBER_NO", DataTypes.STRING())
                                .column("TRANS_ID", DataTypes.STRING())
                                .column("TRANS_DATE", DataTypes.DATE())
                                .column("POST_DATE", DataTypes.DATE())
                                .column("CCY", DataTypes.STRING())
                                .column("TRANS_AMT", DataTypes.DECIMAL(22, 5))
                                .column("TRANS_LCY_AMT", DataTypes.DECIMAL(22, 5))
                                .column("FEE_AMT", DataTypes.DECIMAL(22, 5))
                                .column("PROVINCE", DataTypes.STRING())
                                .column("NATIONALITY", DataTypes.STRING())
                                .column("CAD_TRANS_TYPE", DataTypes.STRING())
                                .column("CAD_TRANS_STATUS", DataTypes.STRING())
                                .column("CAD_AUTH_CODE", DataTypes.STRING())
                                .column("MERCHANT_ID", DataTypes.STRING())
                                .column("TRANS_DETAILS", DataTypes.STRING())
                                .column("TERMINAL_ID", DataTypes.STRING())
                                .column("SOURCE_CHANNEL", DataTypes.STRING())
                                .column("TRANS_CONDITION", DataTypes.STRING())
                                .column("ONUS", DataTypes.STRING())
                                .column("VALUE_TIMESTAMP", DataTypes.STRING())
                                .column("client_id", DataTypes.STRING())
                                .column("rtf_type", DataTypes.STRING())
                                .column("status", DataTypes.STRING())
                                .column("phase", DataTypes.STRING())
                                .column("SYNC_DATETIME_PROCCESS", DataTypes.TIMESTAMP(3))
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
                JsonObject postingInstructionBatch = jsonObj.getAsJsonObject("posting_instruction_batch");
                JsonArray postingInstruction = postingInstructionBatch.getAsJsonArray("posting_instructions");
                int numberOfElements = postingInstruction.size();

                for (int i = 0; i < numberOfElements; i++) {
                        JsonObject postingInstructionObject = postingInstruction.get(i).getAsJsonObject();
                        JsonObject instructionDetails = postingInstructionObject.get("instruction_details")
                                        .getAsJsonObject();
                        JsonArray commitedPosting = postingInstructionObject.getAsJsonArray("committed_postings");
                        for (JsonElement commitedPostingElement : commitedPosting) {
                                Row row = Row.withNames(RowKind.INSERT);
                                JsonObject commitedPostingObject = commitedPostingElement.getAsJsonObject();
                                row.setField("CRD_ACC_ID_ISS", getDataString(instructionDetails, "crd_acc_id_iss"));
                                row.setField("CRD_TARGET_ID", getDataString(instructionDetails, "crd_target_id"));
                                row.setField("CRD_NUNBER_NO", getDataString(instructionDetails, "card_number"));
                                row.setField("TRANS_ID", getDataString(postingInstructionBatch, "client_batch_id"));
                                row.setField("TRANS_DATE", LocalDate.parse(
                                                getDataString(postingInstructionBatch, "value_timestamp")
                                                                .split("T")[0]));
                                row.setField("CCY", getDataString(commitedPostingObject,
                                                "denomination"));
                                boolean isCredit = commitedPostingObject.get("credit")
                                                .getAsBoolean();
                                if (isCredit == false) {
                                        row.setField("TRANS_AMT", getDataNumber(
                                                        commitedPostingElement.getAsJsonObject(), "amount"));
                                        row.setField("TRANS_LCY_AMT", getDataNumber(
                                                        commitedPostingElement.getAsJsonObject(), "amount"));

                                }
                                row.setField("PROVINCE", "");
                                row.setField("NATIONALITY", "");
                                row.setField("CAD_TRANS_TYPE", getDataString(instructionDetails, "channel"));
                                row.setField("CAD_TRANS_STATUS", "");
                                row.setField("CAD_AUTH_CODE", getDataString(instructionDetails, "auth_code"));
                                row.setField("MERCHANT_ID", "");
                                row.setField("TRANS_DETAILS", "");
                                row.setField("TERMINAL_ID", "");
                                row.setField("SOURCE_CHANNEL", getDataString(instructionDetails, "source_channel"));
                                row.setField("TRANS_CONDITION", getDataString(instructionDetails, "trans_condition"));
                                row.setField("ONUS", "");
                                row.setField("VALUE_TIMESTAMP",
                                                getDataString(postingInstructionBatch, "value_timestamp"));
                                // fields for filter
                                row.setField("client_id", getDataString(postingInstructionBatch, "client_id"));
                                row.setField("rtf_type", getDataString(instructionDetails, "rtf_type"));
                                row.setField("status", getDataString(postingInstructionBatch, "status"));
                                row.setField("phase", getDataString(commitedPostingObject, "phase"));
                                row.setField("SYNC_DATETIME_PROCCESS", LocalDateTime.now());
                                rows.add(row);

                        }
                }

                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "CRD_ACC_ID_ISS", "CRD_TARGET_ID", "CRD_NUNBER_NO", "TRANS_ID",
                                                "TRANS_DATE", "POST_DATE", "CCY", "TRANS_AMT", "TRANS_LCY_AMT",
                                                "FEE_AMT", "PROVINCE", "NATIONALITY", "CAD_TRANS_TYPE",
                                                "CAD_TRANS_STATUS",
                                                "CAD_AUTH_CODE", "MERCHANT_ID", "TRANS_DETAILS", "TERMINAL_ID",
                                                "SOURCE_CHANNEL",
                                                "TRANS_CONDITION", "ONUS",
                                                "VALUE_TIMESTAMP", "client_id", "rtf_type", "status", "phase",
                                                "SYNC_DATETIME_PROCCESS" },
                                new TypeInformation[] { Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE, Types.LOCAL_DATE, Types.STRING, Types.BIG_DEC,
                                                Types.BIG_DEC,
                                                Types.BIG_DEC, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING,
                                                Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING,
                                                Types.STRING, Types.STRING,
                                                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE_TIME
                                });
        }
}
