package com.nhannt22.jobs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.nhannt22.SerDes.MessageDeserializerJson;
import com.nhannt22.mapping.PostingInstructionBatchMapping;
import com.nhannt22.mapping.VKAccountBalanceMapping;

import static org.apache.flink.table.api.Expressions.$;


////
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

public class PostingStreaming {
    public static void main(String args[]) {
        try {
            start();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public <E> KafkaSource<Row> consumeKafka(
            Class mappingClass,
            String topic,
            String groupId,
            Properties consumerConfig,
            OffsetResetStrategy offsetResetStrategy
    ) {
        KafkaSource<Row> kafkaSource = KafkaSource.<Row>builder()
                .setTopics(topic)
                .setGroupId(groupId)
                .setProperties(consumerConfig)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(offsetResetStrategy))
                .setDeserializer(new MessageDeserializerJson<>(mappingClass))
                .build();
        return kafkaSource;
    }

    public static void initKafkaTopic(Properties consumerConfig, String topic) {
        // create topic
        try (AdminClient adminClient = AdminClient.create(consumerConfig)) {
            adminClient.createTopics(Arrays.asList(new NewTopic(topic, 5, (short) -1)));
            System.out.println("Topic created");
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    public void SinkKafka(Table inputTable, String sinkTopic, String keyFields, String schemaRegistryUrl,
                          String bootstrapServers, Schema schema) {
        TableResult result = inputTable.executeInsert(TableDescriptor.forConnector("upsert-kafka")
                .schema(schema)
                .option("topic", sinkTopic)
                .option("properties.security.protocol", "SASL_SSL")
                .option("properties.sasl.mechanism", "AWS_MSK_IAM")
                .option("properties.sasl.jaas.config",
                        "software.amazon.msk.auth.iam.IAMLoginModule required;")
                .option("properties.sasl.client.callback.handler.class",
                        "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
                .option("key.format", "raw")
                // .option("key.fields", keyFields)
                .option("value.format", "avro-confluent")
                .option("value.avro-confluent.url",
                        schemaRegistryUrl)
                .option("value.fields-include", "ALL")
                .option("properties.bootstrap.servers", bootstrapServers)
                .build());
        result.print();
    }

    public static void start() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        com.nhannt22.jobs.PostingStreaming flinkApp = new com.nhannt22.jobs.PostingStreaming();

        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.set("lookup.cache.max-rows", "6000");
        tableConfig.set("table.exec.state.ttl", "10800000");

        Properties consumerConfig = new Properties();

//        consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
//        consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
//        consumerConfig.put("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        consumerConfig.put("bootstrap.servers", "54.169.46.195:9094");
        consumerConfig.put("sasl.mechanism", "PLAIN");

//        initKafkaTopic(consumerConfig, outputTopic);

        long current_time = System.currentTimeMillis();
        String groupId = String.valueOf(current_time);

        // Posting Instruction topic
        KafkaSource<Row> kafkPostingIntruction = flinkApp.consumeKafka(
                PostingInstructionBatchMapping.class,
                "raw_tm.vault.api.v1.postings.posting_instruction_batch.created",
                "my-java-application",
                consumerConfig,
                OffsetResetStrategy.EARLIEST
        );

        DataStream<Row> sourcePostingInstruction = env.fromSource(kafkPostingIntruction,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source Posting Instruction Batch");

        Table inputTablePostingIntruction = tableEnv.fromDataStream(sourcePostingInstruction,
                PostingInstructionBatchMapping.schemaOverride());

        tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTablePostingIntruction);

        Table postingDetailsTable = tableEnv.from("POSTING_INSTRUCTION");

        Table resultTable = postingDetailsTable.select($("*"));

        TableResult result = resultTable.execute();
        result.print();


        // Convert Table to DataStream<Row> for the file sink
//        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);
        // Define path for the output CSV file (adjust the path as needed)
//        String outputPath = "/Users/hdb3/Working/FinX/CODE/apache-flink-streaming/src/main/resources/results.csv";
        // Define a file sink with rolling policy
//        final StreamingFileSink<Row> sink = StreamingFileSink
//                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<Row>("UTF-8"))
//                .withRollingPolicy(DefaultRollingPolicy.builder()
//                        .withRolloverInterval(Duration.ofMinutes(15))
//                        .withInactivityInterval(Duration.ofMinutes(5))
//                        .withMaxPartSize(1024 * 1024 * 1024)
//                        .build())
//                .build();
//
//        // Add the sink to the result stream
//        resultStream.addSink(sink);
//
//        // Execute the Flink job
//        env.execute("Export Result Table to CSV");


        Schema output_schema = Schema.newBuilder()
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
                .primaryKey("POSTING_ID")
                .build();

        flinkApp.SinkKafka(
                resultTable,
                "posting_instruction_topic",
                "POSTING_ID",
                "http://54.169.46.195:8085",
                "54.169.46.195:9094",
                output_schema
        );
    }
}
