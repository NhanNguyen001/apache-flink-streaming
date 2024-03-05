package com.nhannt22.table.vkdepositbal.mapping;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.nhannt22.SerDes.MappingFunctionJson;
import com.nhannt22.mapping.TranTypeMapping;
import com.nhannt22.table.vkdepositbal.dto.CommittedPostingDto;
import com.nhannt22.utils.DataUtils;
import com.nhannt22.utils.JsonUtils;
import org.apache.commons.lang3.StringUtils;
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

import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.ACCT_NO;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.AMOUNT;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.GL_CODE_DUCHI;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.ID;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.LOAI_TIEN;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.NARRATIVE;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.SYM_RUN_DATE;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.TRANS_ID;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.TRAN_DATE_TIME;
import static com.nhannt22.table.vkdepositbal.columns.PostingInstructionColumns.VALUE_TIMESTAMP;
import static com.nhannt22.utils.DataUtils.getDataStreamValid;
import static com.nhannt22.utils.DataUtils.isDataValid;
import static com.nhannt22.utils.JsonUtils.concatBtcCode;
import static com.nhannt22.utils.JsonUtils.getDataJsonObject;
import static com.nhannt22.utils.JsonUtils.getDataString;

public class PostingVkDepositBalMapping implements MappingFunctionJson {

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();

                JsonObject payload = input.getAsJsonObject();
                TranTypeMapping tranTypeMapping = new TranTypeMapping();
                JsonObject postingInstructionBatch = payload.getAsJsonObject("posting_instruction_batch");

                String value_timestamp = getDataString(postingInstructionBatch,
                        "value_timestamp");
                String insertionTimestamp = getDataString(postingInstructionBatch,
                        "insertion_timestamp");

                String postingId = getDataString(postingInstructionBatch, "id");
                String vikkiTransactionId = getDataString(postingInstructionBatch, "client_batch_id");

                JsonArray postingInstruction = postingInstructionBatch.getAsJsonArray("posting_instructions");
                int numberOfElements = postingInstruction.size();

                for (int i = 0; i < numberOfElements; i++) {
                        JsonObject postingInstructionObject = postingInstruction.get(i).getAsJsonObject();

                        JsonObject instructionDetails = postingInstructionObject.get("instruction_details")
                                        .getAsJsonObject();
                        String narrative = getDataString(instructionDetails, "narrative");

                        if (StringUtils.isEmpty(narrative)) {
                                narrative = getDataString(instructionDetails, "description");
                        }

                        JsonObject transactionCodeObject = getDataJsonObject(postingInstructionObject,
                                        "transaction_code");

                        String btcCode = concatBtcCode(transactionCodeObject);

                        JsonArray committedPosting = postingInstructionObject.getAsJsonArray("committed_postings");

                        for (JsonElement committedPostingElement : committedPosting) {
                                Row row = Row.withNames(RowKind.INSERT);

                                CommittedPostingDto dto = JsonUtils.fromJsonElement(committedPostingElement, CommittedPostingDto.class);

                                DataUtils.DataStreamValid streamData = getDataStreamValid(dto.getAccountId(), dto.getPhase(), dto.getAccountAddress());

                                if (isDataValid(streamData)) {
                                        String account_id = dto.getAccountId();
                                        row.setField(ID, postingId + account_id);
                                        LocalDate symRunDate = LocalDate.parse(value_timestamp.split("T")[0]);
                                        row.setField(SYM_RUN_DATE, symRunDate);
                                        row.setField(ACCT_NO, account_id);
                                        row.setField(NARRATIVE, narrative);
                                        BigDecimal amount = dto.getAmount();
                                        row.setField(AMOUNT, amount);
                                        row.setField(GL_CODE_DUCHI, "480100007");
                                        row.setField(LOAI_TIEN, dto.getDenomination());
                                        row.setField(TRANS_ID, vikkiTransactionId);
                                        LocalDateTime tranDateTime = LocalDateTime.parse(insertionTimestamp, DateTimeFormatter.ISO_DATE_TIME);
                                        row.setField(TRAN_DATE_TIME, tranDateTime);
                                        LocalDateTime valueTimestamp = LocalDateTime.parse(value_timestamp, DateTimeFormatter.ISO_DATE_TIME);
                                        row.setField(VALUE_TIMESTAMP, valueTimestamp);

                                        rows.add(row);
                                }
                        }
                }

                return rows;
        }



        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] {
                                        SYM_RUN_DATE,
                                        ACCT_NO,
                                        NARRATIVE,
                                        AMOUNT,
                                        LOAI_TIEN,
                                        TRANS_ID,
                                        GL_CODE_DUCHI,
                                        TRAN_DATE_TIME,
                                        VALUE_TIMESTAMP,
                                        ID
                                },
                                new TypeInformation[] {
                                        Types.LOCAL_DATE,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.BIG_DEC,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.STRING,
                                        Types.LOCAL_DATE_TIME,
                                        Types.LOCAL_DATE_TIME,
                                        Types.STRING

                                });
        }
}
