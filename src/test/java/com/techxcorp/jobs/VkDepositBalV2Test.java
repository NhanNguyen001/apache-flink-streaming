package com.nhannt22.jobs;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.nhannt22.table.vkdepositbal.job.FlinkAppVKDepositBalV2;
import com.nhannt22.table.vkdepositbal.mapping.AccountBalanceVkDepositBalMapping;
import com.nhannt22.table.vkdepositbal.mapping.PostingVkDepositBalMapping;
import com.nhannt22.table.vkdepositbal.schema.AccountBalanceSchema;
import com.nhannt22.table.vkdepositbal.schema.PostingInstructionSchema;
import com.nhannt22.table.vkdepositbal.schema.VKDepositBalSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
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

public class VkDepositBalV2Test {

    public  String readFile(String path, Charset encoding) throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    @Test
    public void testApp() {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // Define the path to the JSON file
        String filePathPosting = "src/main/resources/data_example/vk_deposit_bal_data/raw_tm.vault.api.v1.postings.posting_instruction_batch.created.json";
        String filePathAccountBalance = "src/main/resources/data_example/vk_deposit_bal_data/raw_tm.vault.core_api.v1.balances.account_balance.events.json";
        try {
            convertToPosting(filePathPosting, env, tableEnv);
            convertToAccountBalance(filePathAccountBalance, env, tableEnv);
            TableResult tableResult = tableEnv.executeSql(FlinkAppVKDepositBalV2.QUERY_OUTPUT);
            tableResult.print();
//            transferToTopic(tableEnv.sqlQuery(FlinkAppVKDepositBalV2.QUERY_OUTPUT));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void convertToAccountBalance(String filePathAccountBalance, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws IOException {
        AccountBalanceVkDepositBalMapping accountBalanceVkDepositBalMapping = new AccountBalanceVkDepositBalMapping();
        // Read the content of the JSON file into a string
        String jsonContent = readFile(filePathAccountBalance, StandardCharsets.UTF_8);
        // Convert the JSON string to a JsonElement
        JsonElement jsonElement = parseJson(jsonContent);
        List<Row> rows = accountBalanceVkDepositBalMapping.apply(jsonElement, 1L);

        DataStream<Row> rowDataStream = env.fromCollection(rows, accountBalanceVkDepositBalMapping.getProducedType());

        Table inputTableAccountBalance = tableEnv.fromDataStream(rowDataStream,
                AccountBalanceSchema.schema());
        tableEnv.createTemporaryView("ACCOUNT_BALANCE", inputTableAccountBalance);
    }

    private void convertToPosting(String filePathPosting, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws IOException {
        PostingVkDepositBalMapping postingVkDepositBalMapping = new PostingVkDepositBalMapping();
        // Read the content of the JSON file into a string
        String jsonContent = readFile(filePathPosting, StandardCharsets.UTF_8);
        // Convert the JSON string to a JsonElement
        JsonElement jsonElement = parseJson(jsonContent);
        List<Row> rows = postingVkDepositBalMapping.apply(jsonElement, 1231L);

        DataStream<Row> rowDataStream = env.fromCollection(rows, postingVkDepositBalMapping.getProducedType());

        Table inputTableAccountBalance = tableEnv.fromDataStream(rowDataStream,
                PostingInstructionSchema.schema());
        tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTableAccountBalance);
    }

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

    private static JsonElement parseJson(String json) {
        return JsonParser.parseString(json);
    }

    public void transferToTopic(Table inputTable){
        TableResult result = inputTable.executeInsert(TableDescriptor.forConnector("upsert-kafka")
                .schema(VKDepositBalSchema.schema())
                .option("topic","vk_deposit_bal_topic_v2")
                .option("key.format", "raw")
                .option("value.format", "avro-confluent")
                .option("value.avro-confluent.url", "http://localhost:8085")
                .option("value.fields-include", "ALL")
                .option("properties.bootstrap.servers", "http://localhost:9092")
                .build());
        result.print();
    }

}
