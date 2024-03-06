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
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.nhannt22.SerDes.MessageDeserializerJson;
import com.nhannt22.mapping.VKCardDimMapping;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

public class FlinkAppVKCardDim {

        public <E> KafkaSource<Row> consumeKafka(Class mappingClass, String topic,
                        String groupId, Properties consumerConfig, OffsetResetStrategy offsetResetStrategy) {
                KafkaSource<Row> kafkaSource = KafkaSource.<Row>builder()
                                .setTopics(topic)
                                .setGroupId(groupId)
                                .setProperties(consumerConfig)
                                .setStartingOffsets(OffsetsInitializer.committedOffsets(offsetResetStrategy))
                                .setDeserializer(new MessageDeserializerJson<>(mappingClass))
                                .build();
                return kafkaSource;
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

        public static void initKafkaTopic(Properties consumerConfig, String topic) {
                // create topic
                try (AdminClient adminClient = AdminClient.create(consumerConfig)) {
                        adminClient.createTopics(Arrays.asList(new NewTopic(topic, 5, (short) -1)));
                        System.out.println("Topic created");
                } catch (Exception e) {
                        System.out.println(e);
                }
        }

        public JsonObject getSecret(String secretName) {
                SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder()
                                .region(Region.of("ap-southeast-1"))
                                .build();

                // Create a request to get the secret value
                GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                                .secretId(secretName)
                                .build();

                // Get the secret value
                GetSecretValueResponse getSecretValueResponse = secretsManagerClient
                                .getSecretValue(getSecretValueRequest);
                String secret = getSecretValueResponse.secretString();
                Gson gson = new Gson();
                JsonObject secretJson = gson.fromJson(secret, JsonObject.class);
                // secretsManagerClient.close();
                return secretJson;
        }

        public static class LastValueAccumulator {
                public String value;
        }

        public static class LastValueString extends AggregateFunction<String, LastValueAccumulator> {

                @Override
                public String getValue(LastValueAccumulator accumulator) {
                        return accumulator.value;
                }

                @Override
                public LastValueAccumulator createAccumulator() {
                        return new LastValueAccumulator();
                }

                public void accumulate(LastValueAccumulator accumulator, String value) {
                        if ((value != null) && (!value.isEmpty())) {
                                // System.out.printf("value: %s and value length %s\n", value, value.length());
                                accumulator.value = value;
                        }
                }
        }

        public static class LastValueAccumulatorDate {
                public java.sql.Timestamp value;
        }

        public static class LastValueDate extends AggregateFunction<java.sql.Timestamp, LastValueAccumulatorDate> {

                @Override
                public java.sql.Timestamp getValue(LastValueAccumulatorDate accumulator) {
                        return accumulator.value;
                }

                @Override
                public LastValueAccumulatorDate createAccumulator() {
                        return new LastValueAccumulatorDate();
                }

                public void accumulate(LastValueAccumulatorDate accumulator, java.sql.Timestamp value) {
                        if ((value != null) && (!value.toString().isEmpty())) {
                                accumulator.value = value;
                        }
                }
        }



        public static void start(String developingEnv) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                Properties props = new Properties();
                InputStream inputStream = FlinkAppVKCardDim.class.getClassLoader()
                                .getResourceAsStream(developingEnv + "/app.conf");
                try {
                        props.load(inputStream);
                } catch (FileNotFoundException e) {
                        System.out.println(e);
                } catch (IOException e) {
                        System.out.println(e);
                }
                String schemaRegistryUrl = props.getProperty("schema.registry.url");
                String bootstrapServers = props.getProperty("bootstrap.servers");
                String outputTopic = props.getProperty("vk.card.dim.topic") + "_test";
                String groupId = props.getProperty("vk.card.dim.group.id");
                String inputTopic = props.getProperty("card.service.way4.topic");
                // config
                Properties consumerConfig = new Properties();
                consumerConfig.put("security.protocol", "SASL_SSL");
                consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
                consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                consumerConfig.put("sasl.client.callback.handler.class",
                                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
                consumerConfig.put("bootstrap.servers", bootstrapServers);
                initKafkaTopic(consumerConfig, outputTopic);

                FlinkAppVKCardDim flinkApp = new FlinkAppVKCardDim();
                // long current_time = System.currentTimeMillis();
                // String groupId = String.valueOf(current_time);

                KafkaSource<Row> kafkaSource = flinkApp.consumeKafka(
                                VKCardDimMapping.class,
                                inputTopic,
                                groupId, consumerConfig, OffsetResetStrategy.EARLIEST);
                String sourceName = inputTopic;
                DataStream<Row> source = env.fromSource(kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                sourceName);

                Table inputTable = tableEnv.fromDataStream(source, VKCardDimMapping.schemaOverride());
                tableEnv.createTemporarySystemFunction("LAST_VALUE_STRING", new LastValueString());
                tableEnv.createTemporarySystemFunction("LAST_VALUE_DATE", new LastValueDate());
                tableEnv.createTemporaryView("VK_CARD_DIM", inputTable);

                String query = "select  " +
                                "max(SYM_RUN_DATE) as SYM_RUN_DATE, " +
                                "LAST_VALUE_STRING(a.CLIENT_NO) as CLIENT_NO, " +
                                "LAST_VALUE_STRING(a.CLIENT_NAME) as CLIENT_NAME, " +
                                "LAST_VALUE_STRING(a.CRD_CONTRACT_ID) as CRD_CONTRACT_ID, " +
                                "LAST_VALUE_DATE(a.CRD_CONTRACT_OPEN_DATE) as CRD_CONTRACT_OPEN_DATE, " +
                                "LAST_VALUE_STRING(a.CRD_CONTRACT_STATUS) as CRD_CONTRACT_STATUS, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_ID_ISS) as CRD_ACC_ID_ISS, " +
                                "LAST_VALUE_STRING(a.CRD_TARGET_ID) as CRD_TARGET_ID, " +
                                "LAST_VALUE_STRING(a.CRD_NUNBER_NO) as CRD_NUNBER_NO, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_NO) as CRD_ACC_NO, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_STATUS_ISS) as CRD_ACC_STATUS_ISS, " +
                                "LAST_VALUE_DATE(a.CRD_ACC_STATUS_DATE_ISS) as CRD_ACC_STATUS_DATE_ISS, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_OPEN_DATE) as CRD_ACC_OPEN_DATE, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_EXPIRE_DATE) as CRD_ACC_EXPIRE_DATE, " +
                                "LAST_VALUE_DATE(a.CRD_ACC_UNLOCK_DATE) as CRD_ACC_UNLOCK_DATE, " +
                                "LAST_VALUE_DATE(a.CRD_ACC_DESTR) as CRD_ACC_DESTR, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_STATUS) as CRD_ACC_STATUS, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_STATUS_DATE) as CRD_ACC_STATUS_DATE, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_BRAND) as CRD_ACC_BRAND, " +
                                "LAST_VALUE_STRING(a.CRD_ACC_LEVEL) as CRD_ACC_LEVEL, " +
                                "LAST_VALUE(a.CRD_AUTH_AMT_CONTRACT) as CRD_AUTH_AMT_CONTRACT, " +
                                "LAST_VALUE(a.CRD_AUTH_LCY_AMT_CONTRACT) as CRD_AUTH_LCY_AMT_CONTRACT, " +
                                "LAST_VALUE(a.CRD_AUTH_AMT) as CRD_AUTH_AMT, " +
                                "LAST_VALUE(a.CRD_AUTH_LCY_AMT) as CRD_AUTH_LCY_AMT, " +
                                "LAST_VALUE_STRING(a.CRD_GROUP) as CRD_GROUP, " +
                                "LAST_VALUE_STRING(a.CRD_MAIN) as CRD_MAIN, " +
                                "LAST_VALUE_STRING(a.CRD_REPLACE_ID) as CRD_REPLACE_ID, " +
                                "LAST_VALUE_STRING(a.CRD_REPLACE_REASON) as CRD_REPLACE_REASON, " +
                                "LAST_VALUE_DATE(a.CRD_REPLACE_DATE) as CRD_REPLACE_DATE, " +
                                "LAST_VALUE_STRING(a.GL_CODE) as GL_CODE, " +
                                "LAST_VALUE_STRING(a.CRD_ACCOUNT_CASA_ID) as CRD_ACCOUNT_CASA_ID, " +
                                "LAST_VALUE_STRING(a.FLAG_CRD_ACCOUNT_AUTO) as FLAG_CRD_ACCOUNT_AUTO, " +
                                "LAST_VALUE(a.CRD_RATE) as CRD_RATE, " +
                                "LAST_VALUE(a.CRD_OUSTANDING_AMT) as CRD_OUSTANDING_AMT, " +
                                "LAST_VALUE(a.CRD_OUSTANDING_LCY_AMT) as CRD_OUSTANDING_LCY_AMT, " +
                                "LAST_VALUE_DATE(a.SYNC_DATETIME_PROCCES) as SYNC_DATETIME_PROCCES " +
                                "from VK_CARD_DIM a " +
                                "group by a.CRD_TARGET_ID";

                // System.out.println(tableEnv.explainSql(query));
                // TableResult tableResult = tableEnv
                // .executeSql(query);
                // tableResult.print();

                Table joinTable = tableEnv
                                .sqlQuery(query);
                flinkApp.SinkKafka(joinTable,
                                outputTopic, "CRD_TARGET_ID", schemaRegistryUrl, bootstrapServers,
                                VKCardDimMapping.schemaOverride());
                env.execute("FlinkApp_vk_card_dim_" + developingEnv);

        }

        public static void main(String args[]) {
                try {
                        start("uat");
                } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                }
        }
}