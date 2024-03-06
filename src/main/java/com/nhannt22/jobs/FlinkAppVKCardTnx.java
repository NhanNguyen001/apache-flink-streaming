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
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import com.nhannt22.SerDes.MessageDeserializerJson;
import com.nhannt22.mapping.VKCardTnxMapping;

public class FlinkAppVKCardTnx {

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
                TableResult result = inputTable.executeInsert(TableDescriptor.forConnector("kafka")
                                .schema(schema)
                                .option("topic", sinkTopic)
                                .option("properties.security.protocol", "SASL_SSL")
                                .option("properties.sasl.mechanism", "AWS_MSK_IAM")
                                .option("properties.sasl.jaas.config",
                                                "software.amazon.msk.auth.iam.IAMLoginModule required;")
                                .option("properties.sasl.client.callback.handler.class",
                                                "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
                                .option("key.format", "raw")
                                .option("key.fields", keyFields)
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

        public static void start(String developingEnv) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                
                Properties props = new Properties();
                InputStream inputStream = FlinkAppVKCardTnx.class.getClassLoader()
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
                String outputTopic = props.getProperty("vk.card.tnx.topic");
                String groupId = props.getProperty("vk.card.tnx.group.id");
                String inputTopic = props.getProperty("posting.instruction.batch.created.topic");

                // config
                Properties consumerConfig = new Properties();
                consumerConfig.put("security.protocol", "SASL_SSL");
                consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
                consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                consumerConfig.put("sasl.client.callback.handler.class",
                                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
                consumerConfig.put("bootstrap.servers", bootstrapServers);

                initKafkaTopic(consumerConfig, outputTopic);
                

                // long current_time = System.currentTimeMillis();
                // String groupId = String.valueOf(current_time);
                
                FlinkAppVKCardTnx flinkApp = new FlinkAppVKCardTnx();
                KafkaSource<Row> kafkaSource = flinkApp.consumeKafka(
                                VKCardTnxMapping.class,
                                inputTopic,
                                groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                String sourceName = inputTopic;
                DataStream<Row> source = env.fromSource(kafkaSource,
                                WatermarkStrategy.noWatermarks(),
                                sourceName);

                Table inputTable = tableEnv.fromDataStream(source, VKCardTnxMapping.schemaOverride());
                tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTable);

                String query ="SELECT CRD_ACC_ID_ISS, CRD_TARGET_ID, CRD_NUNBER_NO, TRANS_ID, " +
                                                "TRANS_DATE, POST_DATE, CCY, TRANS_AMT, TRANS_LCY_AMT, FEE_AMT, PROVINCE, "
                                                +
                                                "NATIONALITY, CAD_TRANS_TYPE, CAD_TRANS_STATUS, CAD_AUTH_CODE, MERCHANT_ID,"
                                                +
                                                "TRANS_DETAILS, TERMINAL_ID, SOURCE_CHANNEL, TRANS_CONDITION,ONUS, SYNC_DATETIME_PROCCESS " +
                                                "FROM POSTING_INSTRUCTION " 
                                                +
                                                "WHERE client_id LIKE 'card_transactions' " +
                                                "AND status LIKE 'POSTING_INSTRUCTION_BATCH_STATUS_ACCEPTED' ";
                                                //add phase commited, 3 btc code != null
                // TableResult tableResult = tableEnv.executeSql(query);
                // tableResult.print();


                Table joinTable = tableEnv
                                .sqlQuery(query);
                flinkApp.SinkKafka(joinTable,
                                outputTopic, "CRD_ACC_ID_ISS", schemaRegistryUrl, bootstrapServers, null);
                env.execute("FlinkApp_vk_card_tnx_" + developingEnv);

        }
        public static void main(String[] args) throws Exception {
        start("uat");
        }
}