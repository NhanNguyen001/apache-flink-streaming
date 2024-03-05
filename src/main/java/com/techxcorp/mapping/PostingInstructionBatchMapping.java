package com.techxcorp.mapping;

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
import com.techxcorp.SerDes.MappingFunctionJson;

public class PostingInstructionBatchMapping implements MappingFunctionJson {
        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("SYM_RUN_DATE", DataTypes.DATE())
                                .column("BRANCH_NO", DataTypes.STRING())
                                .column("ACCT_NO", DataTypes.STRING().notNull())
                                .column("ACCT_TYPE", DataTypes.STRING())
                                .column("ACCT_TYPE_DESC", DataTypes.STRING())
                                .column("CURRENCY", DataTypes.STRING())
                                .column("CRD_ACC_ID_ISS", DataTypes.STRING())
                                .column("CRD_NUNBER_NO", DataTypes.STRING())
                                .column("TRANS_ID", DataTypes.STRING())
                                .column("TRAN_DATE", DataTypes.DATE())
                                .column("POST_DATE", DataTypes.DATE())
                                .column("POST_STATUS", DataTypes.STRING())
                                .column("TRAN_TYPE", DataTypes.STRING())
                                .column("TRANSACTION_NO", DataTypes.STRING())
                                .column("REVERSAL_TRAN_TYPE", DataTypes.STRING())
                                .column("REVERSAL_DATE", DataTypes.DATE())
                                .column("SOURCE_TYPE", DataTypes.STRING())
                                .column("GL_CODE", DataTypes.STRING())
                                .column("CCY", DataTypes.STRING())
                                .column("TRANS_AMT", DataTypes.DECIMAL(26, 8))
                                .column("TRANS_LCY_AMT", DataTypes.DECIMAL(26, 8))
                                .column("DEBIT_CREDIT_IND", DataTypes.STRING())
                                .column("PSNO", DataTypes.DECIMAL(26, 8))
                                .column("PSCO", DataTypes.DECIMAL(26, 8))
                                .column("PROFIT_CENTRE", DataTypes.STRING())
                                .column("NARRATIVE", DataTypes.STRING())
                                .column("CAD_TRANS_TYPE", DataTypes.STRING())
                                .column("CAD_AUTH_CODE", DataTypes.STRING())
                                .column("client_id", DataTypes.STRING())
                                .column("rtf_type", DataTypes.STRING())
                                .column("status", DataTypes.STRING())
                                .column("PHASE", DataTypes.STRING())
                                .column("VALUE_TIMESTAMP", DataTypes.TIMESTAMP(3))
                                .column("IS_CREDIT", DataTypes.STRING())
                                .column("AMOUNT", DataTypes.DECIMAL(22, 5))
                                .column("ID", DataTypes.STRING().notNull())
                                .column("TRAN_DATE_TIME", DataTypes.TIMESTAMP(3))
                                .column("SENDER_NAME", DataTypes.STRING())
                                .column("RECEIVER_NAME", DataTypes.STRING())
                                .column("SYNC_DATETIME_PROCCES", DataTypes.TIMESTAMP(3))
                                .column("TRANSFER_AMOUNT", DataTypes.DECIMAL(22, 5))
                                .column("POSTING_ID", DataTypes.STRING().notNull())
                                .column("FILTER_TNX", DataTypes.STRING())
                                .watermark("VALUE_TIMESTAMP", "VALUE_TIMESTAMP - INTERVAL '30' SECONDS")
                                // .columnByExpression("NEW_TIME", "PROCTIME()")
                                .primaryKey("ID")
                                .build();
        }

        public String getDataString(JsonObject jsonObject, String fieldName) {
                if (jsonObject == null)
                        return "";
                return jsonObject.has(fieldName)
                                ? jsonObject.get(fieldName).getAsString()
                                : "";
        }

        public BigDecimal getDataNumber(JsonObject jsonObject, String fieldName) {
                if (jsonObject == null)
                        return null;
                return jsonObject.has(fieldName)
                                ? jsonObject.get(fieldName).getAsBigDecimal()
                                : null;
        }

        public JsonObject getDataJsonObject(JsonObject jsonObject, String fieldName) {
                if (jsonObject == null) {
                        return null;
                }
                return jsonObject.has(fieldName) && !jsonObject.get(fieldName).isJsonNull()
                                ? jsonObject.get(fieldName).getAsJsonObject()
                                : null;
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
                        JsonObject transactionCodeObject = getDataJsonObject(postingInstructionObject,
                                        "transaction_code");
                        for (JsonElement commitedPostingElement : commitedPosting) {
                                Row row = Row.withNames(RowKind.INSERT);
                                String client_id = getDataString(postingInstructionBatch, "client_id");
                                String rtf_posting_type = getDataString(instructionDetails, "rtf_posting_type");
                                String account_id = getDataString(commitedPostingElement.getAsJsonObject(),
                                                "account_id");
                                boolean isCredit = commitedPostingElement.getAsJsonObject().get("credit")
                                                .getAsBoolean();
                                String domain = getDataString(transactionCodeObject, "domain");
                                String family = getDataString(transactionCodeObject, "family");
                                String subfamily = getDataString(transactionCodeObject, "subfamily");
                                String code = domain + family + subfamily;
                                TranTypeMapping tranTypeMapping = new TranTypeMapping();
                                LocalDate value_date = LocalDate
                                                .parse(getDataString(postingInstructionBatch, "value_timestamp")
                                                                .split("T")[0]);
                                String status = getDataString(postingInstructionBatch, "status");
                                String phase = getDataString(commitedPostingElement.getAsJsonObject(), "phase");
                                String clientRequestId = getDataString(postingInstructionBatch, "create_request_id");
                                String value_timestamp = getDataString(postingInstructionBatch,
                                                "value_timestamp");
                                LocalDateTime valueTimestamp = LocalDateTime.parse(value_timestamp,
                                                DateTimeFormatter.ISO_DATE_TIME);
                                String insertionTimestamp = getDataString(postingInstructionBatch,
                                                "insertion_timestamp");
                                LocalDateTime insertionTimestampLocalDateTime = LocalDateTime
                                                .parse(insertionTimestamp, DateTimeFormatter.ISO_DATE_TIME);
                                String filter_condition = "0";
                                String instructionEvent = getDataString(instructionDetails, "event");
                                String accountAddress = getDataString(commitedPostingElement.getAsJsonObject(),
                                                "account_address");
                                boolean isContainCode = tranTypeMapping.checkContainCode(code);
                                row.setField("BRANCH_NO", "689");

                                row.setField("ACCT_NO", account_id);
                                row.setField("ACCT_TYPE", "7OV");
                                row.setField("ACCT_TYPE_DESC", "7OV");
                                row.setField("CURRENCY", getDataString(commitedPostingElement.getAsJsonObject(),
                                                "denomination"));
                                row.setField("CRD_ACC_ID_ISS", getDataString(instructionDetails, "reference_number"));
                                row.setField("CRD_NUNBER_NO", getDataString(instructionDetails, "card_number"));
                                row.setField("TRANS_ID", getDataString(postingInstructionBatch, "client_batch_id"));
                                row.setField("TRAN_DATE", LocalDate.parse(
                                                getDataString(postingInstructionBatch, "insertion_timestamp")
                                                                .split("T")[0]));

                                row.setField("TRANSACTION_NO",
                                                getDataString(postingInstructionBatch, "client_batch_id"));

                                row.setField("GL_CODE", "");

                                row.setField("CCY", getDataString(commitedPostingElement.getAsJsonObject(),
                                                "denomination"));

                                if (isCredit == false) {
                                        row.setField("TRANS_AMT", getDataNumber(
                                                        commitedPostingElement.getAsJsonObject(), "amount"));
                                        row.setField("TRANS_LCY_AMT", getDataNumber(
                                                        commitedPostingElement.getAsJsonObject(), "amount"));

                                }
                        
                                if (tranTypeMapping.checkContainCode(code)
                                                && rtf_posting_type.equalsIgnoreCase("AUTHORIZED")) {
                                        row.setField("POST_DATE", null);
                                        row.setField("POST_STATUS", "W");
                                } else if (tranTypeMapping.checkContainCode(code)
                                                && rtf_posting_type.equalsIgnoreCase("SETTLEMENT")) {
                                        row.setField("POST_DATE", value_date);
                                        row.setField("POST_STATUS", "P");
                                } else if (!tranTypeMapping.checkContainCode(code)
                                                && rtf_posting_type.equalsIgnoreCase("AUTHORIZED")) {
                                        row.setField("POST_DATE", value_date);
                                        row.setField("POST_STATUS", "P");
                                }

                                if (isCredit == true) {
                                        row.setField("DEBIT_CREDIT_IND", "CR");
                                        row.setField("TRAN_TYPE",
                                                        tranTypeMapping.getTranType("CR", code));
                                        row.setField("GL_CODE", tranTypeMapping.getGLcode("CR", code));

                                } else if (isCredit == false) {
                                        row.setField("DEBIT_CREDIT_IND", "DR");
                                        row.setField("TRAN_TYPE",
                                                        tranTypeMapping.getTranType("DR", code));
                                        row.setField("GL_CODE", tranTypeMapping.getGLcode("DR", code));
                                }
                                row.setField("SOURCE_TYPE", tranTypeMapping.getSourceType(code));

                                if (rtf_posting_type.equalsIgnoreCase("AUTHORIZED")) {
                                        row.setField("PSNO", BigDecimal.ZERO);
                                } else if (isCredit == false) {
                                        row.setField("PSNO", getDataNumber(
                                                        commitedPostingElement.getAsJsonObject(), "amount"));
                                } else {
                                        row.setField("PSNO", BigDecimal.ZERO);
                                }

                                if (rtf_posting_type.equalsIgnoreCase("AUTHORIZED")) {
                                        row.setField("PSCO", BigDecimal.ZERO);
                                } else if (isCredit == true) {
                                        row.setField("PSCO", getDataNumber(
                                                        commitedPostingElement.getAsJsonObject(), "amount"));
                                } else {
                                        row.setField("PSCO", BigDecimal.ZERO);
                                }

                                row.setField("PROFIT_CENTRE", "201");
                                String narrative = getDataString(instructionDetails, "narrative");
                                String description  = getDataString(instructionDetails, "description");
                                if(narrative.equalsIgnoreCase("")){
                                        row.setField("NARRATIVE", description);
                                }else{
                                        row.setField("NARRATIVE", narrative);
                                }
                                row.setField("NARRATIVE", getDataString(instructionDetails, "narrative"));

                                row.setField("CAD_TRANS_TYPE", getDataString(instructionDetails, "channel"));
                                row.setField("CAD_AUTH_CODE", getDataString(instructionDetails, "auth_code"));
                                row.setField("client_id", client_id);
                                row.setField("rtf_type", getDataString(instructionDetails, "rtf_type"));

                                row.setField("status", status);
                                row.setField("PHASE", phase);

                                row.setField("VALUE_TIMESTAMP", valueTimestamp);

                                row.setField("IS_CREDIT", String.valueOf(isCredit));
                                row.setField("AMOUNT", getDataNumber(
                                                commitedPostingElement.getAsJsonObject(), "amount"));
                                row.setField("ID", getDataString(postingInstructionBatch, "id") + account_id);

                                row.setField("TRAN_DATE_TIME", insertionTimestampLocalDateTime);
                                row.setField("SENDER_NAME", getDataString(instructionDetails, "sender_name"));
                                row.setField("RECEIVER_NAME", getDataString(instructionDetails, "receiver_name"));
                                row.setField("SYNC_DATETIME_PROCCES", LocalDateTime.now());
                                row.setField("TRANSFER_AMOUNT", getDataNumber(
                                                getDataJsonObject(postingInstructionObject, "transfer"), "amount"));
                                row.setField("POSTING_ID",
                                                getDataString(postingInstructionObject, "id") + account_id + status);
                                if (transactionCodeObject == null || isContainCode == false) {
                                        filter_condition = "0";
                                } else if (status.equalsIgnoreCase("POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED")
                                                && phase.equalsIgnoreCase("POSTING_PHASE_COMMITTED")
                                                && clientRequestId.startsWith("SETTLEMENT")) {
                                        filter_condition = "1";
                                } else if (status.equalsIgnoreCase("POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED")
                                                && phase.equalsIgnoreCase("POSTING_PHASE_PENDING_OUTGOING")
                                                && clientRequestId.startsWith("AUTHORIZED")
                                                && client_id.equalsIgnoreCase("card_transactions")) {
                                        filter_condition = "1";
                                } else if (status.equalsIgnoreCase("POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED")
                                                && phase.equalsIgnoreCase("POSTING_PHASE_COMMITTED")
                                                && client_id.equalsIgnoreCase("CoreContracts")) {
                                        filter_condition = "1";
                                } else {
                                        filter_condition = "0";
                                }
                                if (client_id.equalsIgnoreCase("CoreContracts")) {
                                        filter_condition = "0";
                                } else if (instructionEvent.equalsIgnoreCase("CURRENT_ACCOUNT_ACCRUED_INTEREST")
                                                || instructionEvent.equalsIgnoreCase(
                                                                "CURRENT_ACCOUNT_APPLY_ACCRUED_INTEREST")
                                                || instructionEvent.equalsIgnoreCase(
                                                                "CURRENT_ACCOUNT_REVERT_ACCRUAL_INTEREST")) {
                                        if (accountAddress.equalsIgnoreCase("DEFAULT")
                                                        && domain != null
                                                        && family != null
                                                        && subfamily != null) {
                                                filter_condition = "1";
                                        }
                                }
                                
                                row.setField("FILTER_TNX", filter_condition);
                                rows.add(row);

                        }
                }

                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "SYM_RUN_DATE", "BRANCH_NO", "ACCT_NO", "ACCT_TYPE", "ACCT_TYPE_DESC",
                                                "CURRENCY", "CRD_ACC_ID_ISS", "CRD_NUNBER_NO", "TRANS_ID", "TRAN_DATE",
                                                "POST_DATE", "POST_STATUS", "TRAN_TYPE", "TRANSACTION_NO",
                                                "REVERSAL_TRAN_TYPE",
                                                "REVERSAL_DATE", "SOURCE_TYPE", "GL_CODE", "CCY", "TRANS_AMT",
                                                "TRANS_LCY_AMT", "DEBIT_CREDIT_IND", "PSNO", "PSCO", "PROFIT_CENTRE",
                                                "NARRATIVE", "CAD_TRANS_TYPE", "CAD_AUTH_CODE", "client_id", "rtf_type",
                                                "status", "PHASE", "VALUE_TIMESTAMP", "IS_CREDIT",
                                                "AMOUNT", "ID", "TRAN_DATE_TIME", "SENDER_NAME", "RECEIVER_NAME",
                                                "SYNC_DATETIME_PROCCES", "TRANSFER_AMOUNT", "POSTING_ID",
                                                "FILTER_TNX" 
                                        },
                                new TypeInformation[] { Types.LOCAL_DATE, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE, Types.LOCAL_DATE, Types.STRING, Types.STRING,
                                                Types.STRING, Types.STRING, Types.LOCAL_DATE, Types.STRING,
                                                Types.STRING, Types.STRING, Types.BIG_DEC, Types.BIG_DEC, Types.STRING,
                                                Types.BIG_DEC, Types.BIG_DEC, Types.STRING, Types.STRING, Types.STRING,
                                                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE_TIME, Types.STRING, Types.BIG_DEC,
                                                Types.STRING, Types.LOCAL_DATE_TIME, Types.STRING, Types.STRING,
                                                Types.LOCAL_DATE_TIME, Types.BIG_DEC, Types.STRING, 
                                                Types.STRING
                                });
        }
}
