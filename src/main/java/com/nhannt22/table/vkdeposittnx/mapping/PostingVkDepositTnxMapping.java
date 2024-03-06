package com.nhannt22.table.vkdeposittnx.mapping;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.nhannt22.SerDes.MappingFunctionJson;
import com.nhannt22.mapping.TranTypeMapping;
import com.nhannt22.table.vkdeposittnx.dto.CommittedPostingTnxDto;
import com.nhannt22.utils.DataUtils;
import com.nhannt22.utils.DateUtils;
import com.nhannt22.utils.JsonUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CLIENT_NAME;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CLIENT_NO;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.OFFICER_ID;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SYM_RUN_DATE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.BRANCH_NO ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ACCT_NO ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ACCT_TYPE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ACCT_TYPE_DESC ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CURRENCY ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CRD_ACC_ID_ISS ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CRD_NUNBER_NO ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANS_ID ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRAN_DATE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.POST_DATE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.POST_STATUS ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRAN_TYPE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANSACTION_NO ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.REVERSAL_TRAN_TYPE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.REVERSAL_DATE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SOURCE_TYPE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.GL_CODE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CCY ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANS_AMT ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANS_LCY_AMT ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.DEBIT_CREDIT_IND ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PSNO ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PSCO ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PROFIT_CENTRE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.NARRATIVE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CAD_TRANS_TYPE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CAD_AUTH_CODE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.CLIENT_ID ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.RTF_TYPE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.STATUS ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.PHASE ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.VALUE_TIMESTAMP ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.IS_CREDIT ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.AMOUNT ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.ID ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRAN_DATE_TIME ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SENDER_NAME ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.RECEIVER_NAME ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.SYNC_DATETIME_PROCCES ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.TRANSFER_AMOUNT ;
import static com.nhannt22.table.vkdeposittnx.columns.PostingInstructionTnxColumns.POSTING_ID ;
import static com.nhannt22.utils.DataUtils.getDataStreamValid;
import static com.nhannt22.utils.DataUtils.isDataValid;
import static com.nhannt22.utils.DateUtils.getLocalDateTimeNow;
import static com.nhannt22.utils.JsonUtils.concatBtcCode;
import static com.nhannt22.utils.JsonUtils.getDataJsonObject;
import static com.nhannt22.utils.JsonUtils.getDataNumber;
import static com.nhannt22.utils.JsonUtils.getDataString;

public class PostingVkDepositTnxMapping implements MappingFunctionJson {

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();
                JsonObject payload = input.getAsJsonObject();

                TranTypeMapping tranTypeMapping = new TranTypeMapping();

                JsonObject postingInstructionBatch = payload.getAsJsonObject("posting_instruction_batch");
                String clientId = getDataString(postingInstructionBatch, "client_id");
                String valueTimestamp = getDataString(postingInstructionBatch, "value_timestamp");

                String insertionTimestamp = getDataString(postingInstructionBatch, "insertion_timestamp");

                String postingId = getDataString(postingInstructionBatch, "id");
                String vikkiTransactionId = getDataString(postingInstructionBatch, "client_batch_id");
                String status = getDataString(postingInstructionBatch, "status");

                JsonArray postingInstruction = postingInstructionBatch.getAsJsonArray("posting_instructions");
                int numberOfElements = postingInstruction.size();

                for (int i = 0; i < numberOfElements; i++) {
                        JsonObject postingInstructionObject = postingInstruction.get(i).getAsJsonObject();

                        JsonObject instructionDetails = postingInstructionObject.get("instruction_details").getAsJsonObject();

                        String eventType = getDataString(instructionDetails, "event");

                        JsonObject transactionCodeObject = getDataJsonObject(postingInstructionObject,
                                        "transaction_code");

                        String btcCode = concatBtcCode(transactionCodeObject);

                        String narrative = getDataString(instructionDetails, "narrative");

                        if (StringUtils.isEmpty(narrative)) {
                                narrative = getDataString(instructionDetails, "description");
                        }
                        String rtf_posting_type = getDataString(instructionDetails, "rtf_posting_type");

                        JsonArray committedPosting = postingInstructionObject.getAsJsonArray("committed_postings");

                        for (JsonElement committedPostingElement : committedPosting) {
                                Row row = Row.withNames(RowKind.INSERT);

                                CommittedPostingTnxDto dto = JsonUtils.fromJsonElement(committedPostingElement, CommittedPostingTnxDto.class);

                                DataUtils.DataStreamValid streamData = getDataStreamValid(dto.getAccountId(), dto.getPhase(), dto.getAccountAddress());

                                if (isDataValid(streamData)) {
                                        String account_id = dto.getAccountId();
                                        LocalDate symRunDate = LocalDate.parse(valueTimestamp.split("T")[0]);
                                        LocalDate transDate = LocalDate.parse(insertionTimestamp.split("T")[0]);
                                        BigDecimal amount = dto.getAmount();
                                        String creditCode = dto.isCredit() ? "CR" : "DR";
                                        LocalDateTime tranDateTime = LocalDateTime.parse(insertionTimestamp, DateTimeFormatter.ISO_DATE_TIME);
                                        LocalDateTime valueDateTime = LocalDateTime.parse(valueTimestamp, DateTimeFormatter.ISO_DATE_TIME);

                                        row.setField(SYM_RUN_DATE , DateUtils.getLocalDateNow(ZoneId.of("Asia/Ho_Chi_Minh")));
                                        row.setField(BRANCH_NO ,"689");
                                        row.setField(ACCT_NO ,account_id);
                                        row.setField(CLIENT_NO ,null);
                                        row.setField(CLIENT_NAME ,null);
                                        row.setField(ACCT_TYPE ,null);
                                        row.setField(ACCT_TYPE_DESC ,null);
                                        row.setField(CURRENCY ,dto.getDenomination());
                                        row.setField(CRD_ACC_ID_ISS , getDataString(instructionDetails, "reference_number"));
                                        row.setField(CRD_NUNBER_NO ,getDataString(instructionDetails, "card_number"));
                                        row.setField(TRANS_ID , vikkiTransactionId);
                                        row.setField(TRAN_DATE , transDate);

                                        setPostDateAndPostStatus(tranTypeMapping, btcCode, rtf_posting_type, eventType, row, symRunDate);

                                        row.setField(TRAN_TYPE , tranTypeMapping.getTranType(creditCode, btcCode));
                                        row.setField(TRANSACTION_NO , vikkiTransactionId);
                                        row.setField(REVERSAL_TRAN_TYPE ,null);
                                        row.setField(REVERSAL_DATE ,null);
                                        row.setField(SOURCE_TYPE ,tranTypeMapping.getSourceType(btcCode));
                                        row.setField(GL_CODE ,tranTypeMapping.getGLcode(creditCode, btcCode));
                                        row.setField(CCY ,dto.getDenomination());
                                        row.setField(TRANS_AMT , dto.isCredit() ? null : amount);
                                        row.setField(TRANS_LCY_AMT ,dto.isCredit() ? null : amount);
                                        row.setField(DEBIT_CREDIT_IND ,creditCode);
                                        row.setField(PSNO ,dto.isCredit() ? BigDecimal.ZERO : amount);
                                        row.setField(PSCO ,dto.isCredit() ? amount : BigDecimal.ZERO);
                                        row.setField(PROFIT_CENTRE ,"201");
                                        row.setField(NARRATIVE ,narrative);
                                        row.setField(CAD_TRANS_TYPE ,getDataString(instructionDetails, "channel"));
                                        row.setField(CAD_AUTH_CODE ,getDataString(instructionDetails, "auth_code"));
                                        row.setField(CLIENT_ID ,clientId);
                                        row.setField(RTF_TYPE ,getDataString(instructionDetails, "rtf_type"));
                                        row.setField(STATUS ,status);
                                        row.setField(PHASE ,dto.getPhase());
                                        row.setField(VALUE_TIMESTAMP ,valueDateTime);
                                        row.setField(IS_CREDIT ,String.valueOf(dto.isCredit()));
                                        row.setField(AMOUNT ,amount);
                                        row.setField(ID ,postingId + dto.getAccountId());
                                        row.setField(TRAN_DATE_TIME ,tranDateTime);
                                        row.setField(SENDER_NAME ,getDataString(instructionDetails, "sender_name"));
                                        row.setField(RECEIVER_NAME ,getDataString(instructionDetails, "receiver_name"));
                                        row.setField(SYNC_DATETIME_PROCCES ,getLocalDateTimeNow(ZoneId.of("Asia/Ho_Chi_Minh")));
                                        row.setField(TRANSFER_AMOUNT ,getDataNumber(getDataJsonObject(postingInstructionObject, "transfer"), "amount"));
                                        row.setField(POSTING_ID ,postingId+account_id+status);

                                        String officeId = "APP_VIKKI";
                                        if (clientId.equals("card_transactions")) {
                                                officeId = clientId;
                                        }
                                        row.setField(OFFICER_ID ,officeId);

                                        rows.add(row);
                                }
                        }
                }

                return rows;
        }

        private void setPostDateAndPostStatus(TranTypeMapping tranTypeMapping, String btcCode, String rtf_posting_type, String eventType, Row row, LocalDate symRunDate) {
                row.setField(POST_DATE ,null);
                row.setField(POST_STATUS ,null);

                if (tranTypeMapping.checkContainCode(btcCode) &&
                        (
                                rtf_posting_type.equalsIgnoreCase("SETTLEMENT") ||
                                        eventType.startsWith("TIME_DEPOSIT")
                        )
                )
                {
                       row.setField(POST_DATE ,symRunDate);
                       row.setField(POST_STATUS ,"P");
                       return;
                }

                if (!tranTypeMapping.checkContainCode(btcCode) && rtf_posting_type.equalsIgnoreCase("AUTHORIZED")) {
                        row.setField(POST_DATE ,symRunDate);
                        row.setField(POST_STATUS ,"P");
                        return;
                }

                if (tranTypeMapping.checkContainCode(btcCode) && rtf_posting_type.equalsIgnoreCase("AUTHORIZED")) {
                        row.setField(POST_DATE ,null);
                        row.setField(POST_STATUS ,"W");
                }

        }


        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] {
                                        SYM_RUN_DATE,
                                        BRANCH_NO,
                                        ACCT_NO,
                                        ACCT_TYPE,
                                        ACCT_TYPE_DESC,
                                        CURRENCY,
                                        CRD_ACC_ID_ISS,
                                        CRD_NUNBER_NO,
                                        TRANS_ID,
                                        TRAN_DATE,
                                        POST_DATE,
                                        POST_STATUS,
                                        TRAN_TYPE,
                                        TRANSACTION_NO,
                                        REVERSAL_TRAN_TYPE,
                                        REVERSAL_DATE,
                                        SOURCE_TYPE,
                                        GL_CODE,
                                        CCY,
                                        TRANS_AMT,
                                        TRANS_LCY_AMT,
                                        DEBIT_CREDIT_IND,
                                        PSNO,
                                        PSCO,
                                        PROFIT_CENTRE,
                                        NARRATIVE,
                                        CAD_TRANS_TYPE,
                                        CAD_AUTH_CODE,
                                        CLIENT_ID,
                                        RTF_TYPE,
                                        STATUS,
                                        PHASE,
                                        VALUE_TIMESTAMP,
                                        IS_CREDIT,
                                        AMOUNT,
                                        ID,
                                        TRAN_DATE_TIME,
                                        SENDER_NAME,
                                        RECEIVER_NAME,
                                        SYNC_DATETIME_PROCCES,
                                        TRANSFER_AMOUNT,
                                        POSTING_ID,
                                        CLIENT_NO,
                                        CLIENT_NAME,
                                        OFFICER_ID,
                                },
                                new TypeInformation[] {
                                        Types.LOCAL_DATE,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE,
                                        Types.LOCAL_DATE,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.BIG_DEC,
                                        Types.BIG_DEC,
                                        Types.STRING,
                                        Types.BIG_DEC,
                                        Types.BIG_DEC,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.STRING,
                                        Types.BIG_DEC,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.BIG_DEC,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                });
        }
}
