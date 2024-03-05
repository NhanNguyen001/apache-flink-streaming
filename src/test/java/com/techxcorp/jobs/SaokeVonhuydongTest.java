package com.techxcorp.jobs;

import com.google.gson.JsonElement;
import com.techxcorp.table.saokevonduydong.job.FlinkAppSaoKeVonHuyDong;
import com.techxcorp.table.saokevonduydong.mapping.AccountCreatedMapping;
import com.techxcorp.table.saokevonduydong.schema.AccountCreatedSchema;
import com.techxcorp.table.saokevonduydong.schema.SaokeVonHuyDongSchema;
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
import java.time.ZoneId;
import java.util.List;

import static com.techxcorp.utils.JsonUtils.parseJson;

public class SaokeVonhuydongTest {

    public static String readFile(String path, Charset encoding) throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    @Test
    public void testApp() {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));
        // Define the path to the JSON file
        String fileAccountCreated = "src/main/resources/data_example/saoke_vonhuydong/raw_tm.vault.api.v1.accounts.account.created.json";
        try {
            convertToAccountCreated(fileAccountCreated, env, tableEnv);
            TableResult tableResult = tableEnv.executeSql(FlinkAppSaoKeVonHuyDong.QUERY_OUTPUT);

//            Table tableResult = tableEnv.sqlQuery(FlinkAppSaoKeVonHuyDong.QUERY_OUTPUT);
//            transferToTopic(tableResult);
            tableResult.print();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void convertToAccountCreated(String filePathAccountBalance, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) throws IOException {
        AccountCreatedMapping accountCreatedMapping = new AccountCreatedMapping();
        // Read the content of the JSON file into a string
        String jsonContent = readFile(filePathAccountBalance, StandardCharsets.UTF_8);
        // Convert the JSON string to a JsonElement
        JsonElement jsonElement = parseJson(jsonContent);

        List<Row> rows = accountCreatedMapping.apply(jsonElement, 1L);

        DataStream<Row> rowDataStream = env.fromCollection(rows, accountCreatedMapping.getProducedType());

        Table inputTable = tableEnv.fromDataStream(rowDataStream,
                AccountCreatedSchema.schema());

        tableEnv.createTemporaryView("ACCOUNT_CREATED", inputTable);
    }
    public void transferToTopic(Table inputTable){
        TableResult result = inputTable.executeInsert(TableDescriptor.forConnector("upsert-kafka")
                .schema(SaokeVonHuyDongSchema.schema())
                .option("topic","vk.saoke.vonhuydong.topic")
                .option("key.format", "raw")
                .option("value.format", "avro-confluent")
                .option("value.avro-confluent.url", "http://localhost:8085")
                .option("value.fields-include", "ALL")
                .option("properties.bootstrap.servers", "http://localhost:9092")
                .build());
        result.print();
    }

}
