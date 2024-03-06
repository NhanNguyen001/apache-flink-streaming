package com.nhannt22.jobs;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.nhannt22.table.vkdepositbal.schema.VKDepositBalSchema;
import com.nhannt22.table.vkdeposittnx.job.FlinkAppVKDepositTnxV2;
import com.nhannt22.table.vkdeposittnx.mapping.AccountBalanceVkDepositTnxMapping;
import com.nhannt22.table.vkdeposittnx.mapping.PostingVkDepositTnxMapping;
import com.nhannt22.table.vkdeposittnx.schema.AccountBalanceTnxSchema;
import com.nhannt22.table.vkdeposittnx.schema.PostingInstructionTnxSchema;
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

public class VkDepositTnxV2Test {

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
        String filePathPosting = "src/main/resources/data_example/vk_deposit_tnx/raw_tm.vault.api.v1.postings.posting_instruction_batch.created.json";
        String filePathAccountBalance = "src/main/resources/data_example/vk_deposit_tnx/raw_tm.vault.core_api.v1.balances.account_balance.events.json";
        try {
            convertToPosting(filePathPosting, env, tableEnv);
            convertToAccountBalance(filePathAccountBalance, env, tableEnv);
            TableResult tableResult = tableEnv.executeSql(FlinkAppVKDepositTnxV2.QUERY_OUTPUT);
            tableResult.print();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void convertToAccountBalance(String filePathAccountBalance, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws IOException {
        AccountBalanceVkDepositTnxMapping accountBalanceVkDepositBalMapping = new AccountBalanceVkDepositTnxMapping();
        // Read the content of the JSON file into a string
        String jsonContent = readFile(filePathAccountBalance, StandardCharsets.UTF_8);
        // Convert the JSON string to a JsonElement
        JsonElement jsonElement = parseJson(jsonContent);
        List<Row> rows = accountBalanceVkDepositBalMapping.apply(jsonElement, 1L);

        DataStream<Row> rowDataStream = env.fromCollection(rows, accountBalanceVkDepositBalMapping.getProducedType());

        Table inputTableAccountBalance = tableEnv.fromDataStream(rowDataStream,
                AccountBalanceTnxSchema.schema());
        tableEnv.createTemporaryView("ACCOUNT_BALANCE", inputTableAccountBalance);
    }

    private void convertToPosting(String filePathPosting, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws IOException {
        PostingVkDepositTnxMapping postingVkDepositTnxMapping = new PostingVkDepositTnxMapping();
        // Read the content of the JSON file into a string
        String jsonContent = readFile(filePathPosting, StandardCharsets.UTF_8);
        // Convert the JSON string to a JsonElement
        JsonElement jsonElement = parseJson(jsonContent);
        List<Row> rows = postingVkDepositTnxMapping.apply(jsonElement, 1231L);

        DataStream<Row> rowDataStream = env.fromCollection(rows, postingVkDepositTnxMapping.getProducedType());

        Table inputTableAccountBalance = tableEnv.fromDataStream(rowDataStream,
                PostingInstructionTnxSchema.schema());
        tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTableAccountBalance);
    }



    private static JsonElement parseJson(String json) {
        return JsonParser.parseString(json);
    }

    public void transferToTopic(Table inputTable){
        TableResult result = inputTable.executeInsert(TableDescriptor.forConnector("upsert-kafka")
                .schema(VKDepositBalSchema.schema())
                .option("topic","vk_deposit_bal_topic")
                .option("key.format", "raw")
                .option("value.format", "avro-confluent")
                .option("value.avro-confluent.url", "http://localhost:8085")
                .option("value.fields-include", "ALL")
                .option("properties.bootstrap.servers", "http://localhost:9092")
                .build());
        result.print();
    }

}
