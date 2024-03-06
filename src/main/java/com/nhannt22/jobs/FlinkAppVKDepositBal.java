package com.nhannt22.jobs;

import static org.apache.flink.table.api.Expressions.$;

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

import com.nhannt22.SerDes.MessageDeserializerJson;
import com.nhannt22.mapping.PostingInstructionBatchMapping;
import com.nhannt22.mapping.VKAccountBalanceMapping;

public class FlinkAppVKDepositBal {

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

        public static void start(String developingEnv) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                TableConfig tableConfig = tableEnv.getConfig();
                // tableConfig.set("table.exec.state.ttl", "10800000");
                tableConfig.set("lookup.cache.max-rows", "6000");

                Properties props = new Properties();
                InputStream inputStream = FlinkAppVKDepositTnx.class.getClassLoader()
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
                String outputTopic = props.getProperty("vk.deposit.bal.topic");
                // String groupId = props.getProperty("vk.deposit.bal.group.id");
                // config
                Properties consumerConfig = new Properties();
                consumerConfig.put("security.protocol", "SASL_SSL");
                consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
                consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                consumerConfig.put("sasl.client.callback.handler.class",
                                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
                consumerConfig.put("bootstrap.servers", bootstrapServers);

                initKafkaTopic(consumerConfig, outputTopic);
                // String groupId = "non_prod_flink-vk-deposit-bal-group-202311140325";

                long current_time = System.currentTimeMillis();
                String groupId = String.valueOf(current_time);

                FlinkAppVKDepositBal flinkApp = new FlinkAppVKDepositBal();

                // tableEnv.executeSql("CREATE TEMPORARY TABLE ACCOUNT_EVENT ( " + //
                //                 "  CLIENT_NO STRING, " + //
                //                 "  CLIENT_NAME STRING, " + //
                //                 "  ACCT_NO STRING, " + //
                //                 "  PRODUCT_ID STRING, " +
                //                 "  PROC_TIME TIMESTAMP(3), " + //
                //                 "  WATERMARK FOR PROC_TIME AS PROC_TIME - INTERVAL '15' SECOND," +
                //                 "  PRIMARY KEY(ACCT_NO) NOT ENFORCED" +
                //                 ") WITH ( " + //
                //                 "  'connector' = 'jdbc', " + //
                //                 "  'url' = 'jdbc:oracle:thin:@//10.0.119.13:1521/FINX', " + //
                //                 "  'table-name' = 'VK_ACCOUNT_CREATED', " + //
                //                 "  'username' = 'UAT', " + //
                //                 "  'password' = 'uat$1234', " + //
                //                 "  'driver'   = 'oracle.jdbc.driver.OracleDriver'" +
                //                 ");");
                // tableEnv.executeSql("CREATE TEMPORARY TABLE PRODUCT_INFO ( " + //
                //                 "  PRODUCT_ID STRING, " + //
                //                 "  PARAM_NAME STRING, " + //
                //                 "  PARAM_VALUE DECIMAL, " + //
                //                 "  PROC_TIME TIMESTAMP(3), " + //
                //                 "  WATERMARK FOR PROC_TIME AS PROC_TIME - INTERVAL '15' SECOND," +
                //                 "  PRIMARY KEY(PRODUCT_ID) NOT ENFORCED" +
                //                 ") WITH ( " + //
                //                 "  'connector' = 'jdbc', " + //
                //                 "  'url' = 'jdbc:oracle:thin:@//10.0.119.13:1521/FINX', " + //
                //                 "  'table-name' = 'VK_PRODUCT_INFO', " + //
                //                 "  'username' = 'UAT', " + //
                //                 "  'password' = 'uat$1234', " + //
                //                 "  'driver'   = 'oracle.jdbc.driver.OracleDriver'" +
                //                 ");");

                // raw_tm.vault.core_api.v1.balances.account_balance.events
                KafkaSource<Row> kafkaAccountBalance = flinkApp.consumeKafka(
                                VKAccountBalanceMapping.class,
                                "raw_tm.vault.core_api.v1.balances.account_balance.events",
                                groupId, consumerConfig, OffsetResetStrategy.EARLIEST);
                DataStream<Row> sourceAccountBalance = env.fromSource(kafkaAccountBalance,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Source 2")
                                .filter(row -> row.getField("ACCOUNT_ID").toString()
                                                .startsWith("88")
                                                &&
                                                row.getField("PHASE").toString()
                                                                .equalsIgnoreCase("POSTING_PHASE_COMMITTED"));
                Table inputTableAccountBalance = tableEnv.fromDataStream(sourceAccountBalance,
                                VKAccountBalanceMapping.schemaOverride());
                tableEnv.createTemporaryView("ACCOUNT_BALANCE", inputTableAccountBalance);

                KafkaSource<Row> kafkPostingIntruction = flinkApp.consumeKafka(
                                PostingInstructionBatchMapping.class,
                                "raw_tm.vault.api.v1.postings.posting_instruction_batch.created",
                                groupId, consumerConfig, OffsetResetStrategy.EARLIEST);
                DataStream<Row> sourcePostingIntruction = env.fromSource(kafkPostingIntruction,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Source Posting Instruction Batch")
                                .filter(row -> (row.getField("SOURCE_TYPE").toString()
                                                .equalsIgnoreCase("card_transactions") ||
                                                row.getField("SOURCE_TYPE").toString()
                                                                .equalsIgnoreCase("internal_transfer")
                                                ||
                                                row.getField("SOURCE_TYPE").toString()
                                                                .equalsIgnoreCase("interbank_transfer"))
                                                &&
                                                row.getField("ACCT_NO").toString()
                                                                .startsWith("88")
                                                &&
                                                row.getField("PHASE").toString()
                                                                .equalsIgnoreCase("POSTING_PHASE_COMMITTED"));
                Table inputTablePostingIntruction = tableEnv.fromDataStream(sourcePostingIntruction,
                                PostingInstructionBatchMapping.schemaOverride());
                tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTablePostingIntruction);

                String query = "SELECT p.SYM_RUN_DATE, p.ACCT_NO, SODU_NT, SODU_QD, "+
                                                "p.VALUE_TIMESTAMP as TRAN_DATE_TIME, " +
                                                "p.TRANS_ID as VIKKI_TRANSACTION_ID, " +
                                                "p.NARRATIVE, " +
                                                "p.TRANSFER_AMOUNT as AMOUNT, " +
                                                "SODU_NT as ACTUAL_BAL, " +
                                                "p.SYNC_DATETIME_PROCCES, " +
                                                "p.POSTING_ID " +
                                                "FROM POSTING_INSTRUCTION p LEFT JOIN ACCOUNT_BALANCE ab on p.ID = ab.ID  " +
                                                "WHERE p.VALUE_TIMESTAMP BETWEEN ab.VALUE_TIMESTAMP - INTERVAL '10' MINUTES AND ab.VALUE_TIMESTAMP + INTERVAL '10' MINUTES ";

                // TableResult tmp = tableEnv.executeSql(query);
                // tmp.print();
                Schema schema = Schema.newBuilder()
                                .column("SYM_RUN_DATE", DataTypes.DATE())
                                // .column("CLIENT_NO", DataTypes.STRING())
                                // .column("CLIENT_NAME", DataTypes.STRING())
                                .column("ACCT_NO", DataTypes.STRING())
                                .column("SODU_NT", DataTypes.DECIMAL(22, 5))
                                .column("SODU_QD", DataTypes.DECIMAL(22, 5))
                                // .column("LAISUAT", DataTypes.DOUBLE())
                                .column("TRAN_DATE_TIME", DataTypes.TIMESTAMP(3))
                                .column("VIKKI_TRANSACTION_ID", DataTypes.STRING())
                                .column("NARRATIVE", DataTypes.STRING())
                                .column("AMOUNT", DataTypes.DECIMAL(22, 5))
                                .column("ACTUAL_BAL", DataTypes.DECIMAL(22, 5))
                                .column("SYNC_DATETIME_PROCCES", DataTypes.TIMESTAMP(3))
                                .column("POSTING_ID", DataTypes.STRING().notNull())
                                .primaryKey("POSTING_ID")
                                .build();
                Table joinTable = tableEnv
                                .sqlQuery(query);
                flinkApp.SinkKafka(joinTable,
                                outputTopic, "POSTING_ID", schemaRegistryUrl, bootstrapServers, schema);
                env.execute("FlinkApp_vk_deposit_bal_" + developingEnv);

        }

        public static void main(String args[]) {
                // try {
                //         start("uat");
                // } catch (Exception e) {
                //         // TODO Auto-generated catch block
                //         e.printStackTrace();
                // }

        }
}