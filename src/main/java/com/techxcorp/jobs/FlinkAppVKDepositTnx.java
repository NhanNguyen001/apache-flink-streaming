package com.techxcorp.jobs;

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
import com.techxcorp.SerDes.MessageDeserializerJson;
import com.techxcorp.mapping.PostingInstructionBatchMapping;
import com.techxcorp.mapping.VKAccountBalanceMapping;

public class FlinkAppVKDepositTnx {

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

        public static void start(String developingEnv) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
                FlinkAppVKDepositTnx flinkApp = new FlinkAppVKDepositTnx();
                TableConfig tableConfig = tableEnv.getConfig();
                tableConfig.set("lookup.cache.max-rows", "6000");
                // tableConfig.set("table.exec.state.ttl","10800000");

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
                inputStream.close();
                String schemaRegistryUrl = props.getProperty("schema.registry.url");
                String bootstrapServers = props.getProperty("bootstrap.servers");
                String secretName = props.getProperty("secret.manager");
                String outputTopic = props.getProperty("vk.deposit.tnx.topic");
                // String groupId = props.getProperty("vk.deposit.tnx.group.id");
                JsonObject secretJson = flinkApp.getSecret(secretName);
                String tnsname = secretJson.get("tnsname").getAsString();
                String username = secretJson.get("username").getAsString();
                String password = secretJson.get("password").getAsString();
                
                // String tnsname = "10.0.119.13:1521/FINX";
                // String username = "UAT";
                // String password = "uat$1234";
                // config
                Properties consumerConfig = new Properties();
                consumerConfig.put("security.protocol", "SASL_SSL");
                consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
                consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
                consumerConfig.put("sasl.client.callback.handler.class",
                                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
                consumerConfig.put("bootstrap.servers", bootstrapServers);

                initKafkaTopic(consumerConfig, outputTopic);

                long current_time = System.currentTimeMillis();
                String groupId = String.valueOf(current_time);

                String query_account_event = String.format("CREATE TEMPORARY TABLE ACCOUNT_EVENT ( " + //
                                "  CLIENT_NO STRING, " + //
                                "  ACCT_NO STRING, " + //
                                "  PROC_TIME TIMESTAMP(3), " + //
                                "  WATERMARK FOR PROC_TIME AS PROC_TIME - INTERVAL '15' SECOND," +
                                "  PRIMARY KEY(ACCT_NO) NOT ENFORCED" +
                                ") WITH ( " + //
                                "  'connector' = 'jdbc', " + //
                                "  'url' = 'jdbc:oracle:thin:@//%s', " + //
                                "  'table-name' = 'VK_ACCOUNT_CREATED', " + //
                                "  'username' = '%s', " + //
                                "  'password' = '%s', " + //
                                "  'driver'   = 'oracle.jdbc.driver.OracleDriver'" +
                                ");", tnsname, username, password);
                String query_vk_client = String.format("CREATE TEMPORARY TABLE VK_CLIENT ( " + //
                                "  CLIENT_NO STRING, " + //
                                "  CLIENT_NAME STRING " + //
                                ") WITH ( " + //
                                "  'connector' = 'jdbc', " + //
                                "  'url' = 'jdbc:oracle:thin:@//%s', " + //
                                "  'table-name' = 'VK_CLIENT_REALTIME', " + //
                                "  'username' = '%s', " + //
                                "  'password' = '%s', " + //
                                "  'driver'   = 'oracle.jdbc.driver.OracleDriver'" +
                                ");", tnsname, username, password);
                // Lookup VK_ACCOUNT_CREATED
                tableEnv.executeSql(query_account_event);

                // Lookup VK_CLIENT
                tableEnv.executeSql(query_vk_client);

                // Posting Instruction topic
                KafkaSource<Row> kafkPostingIntruction = flinkApp.consumeKafka(
                                PostingInstructionBatchMapping.class,
                                "raw_tm.vault.api.v1.postings.posting_instruction_batch.created",
                                groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                DataStream<Row> sourcePostingIntruction = env.fromSource(kafkPostingIntruction,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Source Posting Instruction Batch")
                                .filter(row -> (row.getField("FILTER_TNX").toString()
                                                .equalsIgnoreCase("1")));
                Table inputTablePostingIntruction = tableEnv.fromDataStream(sourcePostingIntruction,
                                PostingInstructionBatchMapping.schemaOverride());
                tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTablePostingIntruction);

                // Account Balance topic
                KafkaSource<Row> kafkAccountBalance = flinkApp.consumeKafka(
                                VKAccountBalanceMapping.class,
                                "raw_tm.vault.core_api.v1.balances.account_balance.events",
                                groupId, consumerConfig, OffsetResetStrategy.EARLIEST);
                DataStream<Row> sourceAccountBalance = env.fromSource(kafkAccountBalance,
                                WatermarkStrategy.noWatermarks(),
                                "Kafka Source Account Balance")
                                .filter(row -> row.getField("FILTER_TNX").toString()
                                                .equalsIgnoreCase("1"));
                Table inputTableAccountBalance = tableEnv.fromDataStream(sourceAccountBalance,
                                VKAccountBalanceMapping.schemaOverride()).distinct();
                tableEnv.createTemporaryView("ACCOUNT_BALANCE", inputTableAccountBalance);

                String query = "SELECT  " + //
                                "    p.POST_DATE as SYM_RUN_DATE,   " + //
                                "    BRANCH_NO,   " + //
                                "    ae.CLIENT_NO,   " + //
                                "    vc.CLIENT_NAME,   " + //
                                "    p.ACCT_NO,   " + //
                                "    ACCT_TYPE,   " + //
                                "    ACCT_TYPE_DESC,   " + //
                                "    CURRENCY,   " + //
                                "    TRAN_DATE,   " + //
                                "    TRAN_DATE_TIME,   " + //
                                "    TRAN_TYPE,   " + //
                                "    p.TRANS_ID as TRANSACTION_NO,   " + //
                                "    POST_DATE,   " + //
                                "    POST_STATUS,   " + //
                                "    REVERSAL_TRAN_TYPE,   " + //
                                "    REVERSAL_DATE,   " + //
                                "    SOURCE_TYPE,   " + //
                                "    GL_CODE,   " + //
                                "    ab.SODU_NT-PSCO+PSNO as PREVIOUS_BAL_AMT,   " + //
                                "    DEBIT_CREDIT_IND,   " + //
                                "    PSNO,   " + //
                                "    PSCO,   " + //
                                "    ab.SODU_NT as ACTUAL_BAL_AMT,   " + //
                                "    case when p.client_id = 'card_transactions' then ae.CLIENT_NO else 'APP_VIKKI' end as OFFICER_ID,   "
                                + //
                                "    PROFIT_CENTRE,   " + //
                                "    NARRATIVE,  " + //
                                "    p.POSTING_ID, " + //
                                "    p.SYNC_DATETIME_PROCCES" +
                                "    FROM POSTING_INSTRUCTION p LEFT JOIN ACCOUNT_BALANCE ab " +
                                "    ON p.ID = ab.ID AND p.VALUE_TIMESTAMP BETWEEN ab.VALUE_TIMESTAMP - INTERVAL '10' MINUTES AND ab.VALUE_TIMESTAMP + INTERVAL '10' MINUTES "
                                +
                                "    LEFT JOIN  ACCOUNT_EVENT AS ae ON p.ACCT_NO = ae.ACCT_NO  " +
                                "    JOIN VK_CLIENT AS vc ON vc.CLIENT_NO = ae.CLIENT_NO ";
                String query1 = "select * from VK_CLIENT_REALTIME where ";
                String query2 = "SELECT  " + //
                // " ab.SODU_NT-PSCO+PSNO as PREVIOUS_BAL_AMT, " + //
                                "    ae.CLIENT_NO,   " + //
                                "    vc.CLIENT_NAME,   " + //
                                "    TRANSACTION_NO,   " + //
                                "    SOURCE_TYPE,   " + //
                                "    p.ID, " +
                                "    p.TRAN_TYPE, " +
                                "    p.SYNC_DATETIME_PROCCES" +
                                "    FROM POSTING_INSTRUCTION p LEFT JOIN ACCOUNT_BALANCE ab " +
                                "    ON p.ID = ab.ID AND p.VALUE_TIMESTAMP BETWEEN ab.VALUE_TIMESTAMP - INTERVAL '10' MINUTES AND ab.VALUE_TIMESTAMP + INTERVAL '10' MINUTES "
                                +
                                "    LEFT JOIN  ACCOUNT_EVENT AS ae ON p.ACCT_NO = ae.ACCT_NO  " +
                                "    JOIN VK_CLIENT AS vc ON vc.CLIENT_NO = ae.CLIENT_NO "+
                                "    WHERE TRANSACTION_NO = 'CA2312050000067'";
                // tableEnv.explainSql(query);
                // TableResult tmp = tableEnv.executeSql(query2);
                // tmp.print();

                Schema output_schema = Schema.newBuilder()
                                .column("SYM_RUN_DATE", DataTypes.DATE())
                                .column("BRANCH_NO", DataTypes.STRING())
                                .column("CLIENT_NO", DataTypes.STRING())
                                .column("CLIENT_NAME", DataTypes.STRING())
                                .column("ACCT_NO", DataTypes.STRING().notNull())
                                .column("ACCT_TYPE", DataTypes.STRING())
                                .column("ACCT_TYPE_DESC", DataTypes.STRING())
                                .column("CURRENCY", DataTypes.STRING())
                                .column("TRAN_DATE", DataTypes.DATE())
                                .column("TRAN_DATE_TIME", DataTypes.TIMESTAMP(3))
                                .column("TRAN_TYPE", DataTypes.STRING())
                                .column("TRANSACTION_NO", DataTypes.STRING())
                                .column("POST_DATE", DataTypes.DATE())
                                .column("POST_STATUS", DataTypes.STRING())
                                .column("REVERSAL_TRAN_TYPE", DataTypes.STRING())
                                .column("REVERSAL_DATE", DataTypes.DATE())
                                .column("SOURCE_TYPE", DataTypes.STRING())
                                .column("GL_CODE", DataTypes.STRING())
                                .column("PREVIOUS_BAL_AMT", DataTypes.DECIMAL(26, 8))
                                .column("DEBIT_CREDIT_IND", DataTypes.STRING())
                                .column("PSNO", DataTypes.DECIMAL(26, 8))
                                .column("PSCO", DataTypes.DECIMAL(26, 8))
                                .column("ACTUAL_BAL_AMT", DataTypes.DECIMAL(26, 8))
                                .column("OFFICER_ID", DataTypes.STRING())
                                .column("PROFIT_CENTRE", DataTypes.STRING())
                                .column("NARRATIVE", DataTypes.STRING())
                                .column("POSTING_ID", DataTypes.STRING().notNull())
                                .column("SYNC_DATETIME_PROCCES", DataTypes.TIMESTAMP(3))
                                .primaryKey("POSTING_ID")
                                .build();

                Table joinTable = tableEnv.sqlQuery(query);
                flinkApp.SinkKafka(joinTable,
                                outputTopic, "POSTING_ID", schemaRegistryUrl, bootstrapServers,
                                output_schema);
                env.execute("FlinkApp_vk_deposit_tnx" + developingEnv);

        }

        public static void main(String args[]) {
                // try {
                // start("uat");
                // } catch (Exception e) {
                // // TODO Auto-generated catch block
                // e.printStackTrace();
                // }


        }
}