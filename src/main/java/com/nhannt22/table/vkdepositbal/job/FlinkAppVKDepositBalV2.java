package com.nhannt22.table.vkdepositbal.job;

import com.nhannt22.exception.FlinkException;
import com.nhannt22.table.vkdepositbal.mapping.AccountBalanceVkDepositBalMapping;
import com.nhannt22.table.vkdepositbal.mapping.PostingVkDepositBalMapping;
import com.nhannt22.table.vkdepositbal.schema.AccountBalanceSchema;
import com.nhannt22.table.vkdepositbal.schema.PostingInstructionSchema;
import com.nhannt22.table.vkdepositbal.schema.VKDepositBalSchema;
import com.nhannt22.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.ZoneId;
import java.util.Map;
import java.util.Properties;

import static com.nhannt22.constant.RawTopicConstant.RAW_TOPIC_ACCOUNT_BALANCE;
import static com.nhannt22.constant.RawTopicConstant.RAW_TOPIC_POSTING_INSTRUCTION;
import static com.nhannt22.utils.FileUtils.readFilePropertiers;
import static com.nhannt22.utils.KafkaUtils.getConfigKafka;
import static com.nhannt22.utils.KafkaUtils.initKafkaTopic;
import static java.lang.String.format;

public class FlinkAppVKDepositBalV2 {

        public static final String QUERY_OUTPUT = "SELECT " +
                " POSTING_INSTRUCTION.SYM_RUN_DATE, " +
                " POSTING_INSTRUCTION.ACCT_NO, " +
                " ACCOUNT_BALANCE.SODU_NT, " +
                " ACCOUNT_BALANCE.SODU_QD, "+
                " POSTING_INSTRUCTION.TRAN_DATE_TIME, " +
                " POSTING_INSTRUCTION.TRANS_ID AS  VIKKI_TRANSACTION_ID, " +
                " POSTING_INSTRUCTION.NARRATIVE, " +
                " POSTING_INSTRUCTION.AMOUNT, " +
                " ACCOUNT_BALANCE.SODU_NT AS ACTUAL_BAL, " +
                " POSTING_INSTRUCTION.ID, " +
                " POSTING_INSTRUCTION.LOAI_TIEN, " +
                " POSTING_INSTRUCTION.GL_CODE_DUCHI, "+
                " NOW() AS SYNC_DATETIME_PROCCES " +
                "FROM POSTING_INSTRUCTION " +
                "LEFT OUTER JOIN ACCOUNT_BALANCE " +
                "ON POSTING_INSTRUCTION.ID = ACCOUNT_BALANCE.ID " +
                "WHERE POSTING_INSTRUCTION.VALUE_TIMESTAMP BETWEEN ACCOUNT_BALANCE.VALUE_TIMESTAMP - INTERVAL '5' MINUTE AND ACCOUNT_BALANCE.VALUE_TIMESTAMP + INTERVAL '5' MINUTE ";

        public static void start(String developingEnv) {

                try {
                        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                        TableConfig tableConfig = tableEnv.getConfig();
                        tableConfig.set("lookup.cache.max-rows", "6000");
                        tableConfig.setLocalTimeZone(ZoneId.of("UTC"));

                        Properties props = readFilePropertiers(developingEnv);

                        String schemaRegistryUrl = props.getProperty("schema.registry.url");
                        String bootstrapServers = props.getProperty("bootstrap.servers");
                        String outputTopic = props.getProperty("vk.deposit.bal.topic");
                        String groupId = props.getProperty("vk.deposit.bal.group.id");
                        // config
                        Properties consumerConfig = getConfigKafka(bootstrapServers, Map.of());

                        initKafkaTopic(consumerConfig, outputTopic);

                        convertToAccountBalance(groupId, consumerConfig, env, tableEnv);

                        convertToPostingInstruction(groupId, consumerConfig, env, tableEnv);

                        Table joinTable = tableEnv
                                .sqlQuery(QUERY_OUTPUT);

                        KafkaUtils.SinkKafka(joinTable, outputTopic, schemaRegistryUrl, bootstrapServers, VKDepositBalSchema.schema());
                        env.execute("FlinkApp_vk_deposit_bal_" + developingEnv);
                }catch (Exception exception){
                        throw new FlinkException("Execute job FlinkApp_vk_deposit_bal" + format("%s failed %s",developingEnv, exception.getMessage()));
                }


        }
        private static void convertToPostingInstruction(String groupId, Properties consumerConfig, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

                KafkaSource<Row> kafkaPostingInstruction = KafkaUtils.consumeKafka(
                        PostingVkDepositBalMapping.class,
                        RAW_TOPIC_POSTING_INSTRUCTION,
                        groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                DataStream<Row> sourcePostingInstruction = env.fromSource(kafkaPostingInstruction,
                                WatermarkStrategy.noWatermarks(),
                                RAW_TOPIC_POSTING_INSTRUCTION);

                Table inputTablePostingInstruction = tableEnv.fromDataStream(sourcePostingInstruction,
                        PostingInstructionSchema.schema());

                tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTablePostingInstruction);
        }


        private static void convertToAccountBalance(String groupId, Properties consumerConfig, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
                KafkaSource<Row> kafkaAccountBalance = KafkaUtils.consumeKafka(
                        AccountBalanceVkDepositBalMapping.class,
                        RAW_TOPIC_ACCOUNT_BALANCE,
                        groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                DataStream<Row> sourceAccountBalance = env.fromSource(kafkaAccountBalance,
                                WatermarkStrategy.noWatermarks(),
                                RAW_TOPIC_ACCOUNT_BALANCE);

                Table inputTableAccountBalance = tableEnv.fromDataStream(sourceAccountBalance,
                        AccountBalanceSchema.schema());

                tableEnv.createTemporaryView("ACCOUNT_BALANCE", inputTableAccountBalance);
        }
}