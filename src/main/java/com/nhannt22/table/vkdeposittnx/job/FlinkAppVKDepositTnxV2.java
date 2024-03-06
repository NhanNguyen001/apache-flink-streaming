package com.nhannt22.table.vkdeposittnx.job;

import com.nhannt22.table.vkdeposittnx.mapping.AccountBalanceVkDepositTnxMapping;
import com.nhannt22.table.vkdeposittnx.mapping.PostingVkDepositTnxMapping;
import com.nhannt22.table.vkdeposittnx.schema.AccountBalanceTnxSchema;
import com.nhannt22.table.vkdeposittnx.schema.PostingInstructionTnxSchema;
import com.nhannt22.table.vkdeposittnx.schema.VKDepositTnxSchema;
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

public class FlinkAppVKDepositTnxV2 {

        public static final String QUERY_OUTPUT = "SELECT " +
                " POSTING_INSTRUCTION.SYM_RUN_DATE , " +
                " POSTING_INSTRUCTION.BRANCH_NO, " +
                " POSTING_INSTRUCTION.CLIENT_NO, " +
                " POSTING_INSTRUCTION.CLIENT_NAME, " +
                " POSTING_INSTRUCTION.ACCT_NO, " +
                " POSTING_INSTRUCTION.ACCT_TYPE, " +
                " POSTING_INSTRUCTION.ACCT_TYPE_DESC, " +
                " POSTING_INSTRUCTION.CURRENCY, " +
                " POSTING_INSTRUCTION.TRAN_DATE, " +
                " POSTING_INSTRUCTION.TRAN_DATE_TIME, " +
                " POSTING_INSTRUCTION.TRAN_TYPE, " +
                " POSTING_INSTRUCTION.TRANS_ID as TRANSACTION_NO, " +
                " POSTING_INSTRUCTION.POST_DATE, " +
                " POSTING_INSTRUCTION.POST_STATUS, " +
                " POSTING_INSTRUCTION.REVERSAL_TRAN_TYPE, " +
                " POSTING_INSTRUCTION.REVERSAL_DATE, " +
                " POSTING_INSTRUCTION.SOURCE_TYPE, " +
                " POSTING_INSTRUCTION.GL_CODE, " +
                " (ACCOUNT_BALANCE.SODU_NT - PSCO + PSNO) as PREVIOUS_BAL_AMT, " +
                " POSTING_INSTRUCTION.DEBIT_CREDIT_IND, " +
                " POSTING_INSTRUCTION.PSNO, " +
                " POSTING_INSTRUCTION.PSCO, " +
                " ACCOUNT_BALANCE.SODU_NT as ACTUAL_BAL_AMT, " +
                " POSTING_INSTRUCTION.OFFICER_ID, " +
                " POSTING_INSTRUCTION.PROFIT_CENTRE, " +
                " POSTING_INSTRUCTION.NARRATIVE, " +
                " POSTING_INSTRUCTION.POSTING_ID, " +
                " POSTING_INSTRUCTION.SYNC_DATETIME_PROCCES " +
                "FROM POSTING_INSTRUCTION " +
                "LEFT OUTER JOIN ACCOUNT_BALANCE " +
                "ON POSTING_INSTRUCTION.ID = ACCOUNT_BALANCE.ID " +
                "WHERE POSTING_INSTRUCTION.VALUE_TIMESTAMP BETWEEN ACCOUNT_BALANCE.VALUE_TIMESTAMP - INTERVAL '5' MINUTE AND ACCOUNT_BALANCE.VALUE_TIMESTAMP + INTERVAL '5' MINUTE ";

        public static void start(String developingEnv) throws Exception {
                final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

                TableConfig tableConfig = tableEnv.getConfig();

                tableConfig.set("lookup.cache.max-rows", "6000");
                tableConfig.setLocalTimeZone(ZoneId.of("UTC"));

                Properties props = readFilePropertiers(developingEnv);

                String schemaRegistryUrl = props.getProperty("schema.registry.url");
                String bootstrapServers = props.getProperty("bootstrap.servers");
                String outputTopic = props.getProperty("vk.deposit.tnx.topic");
                String groupId = props.getProperty("vk.deposit.tnx.group.id");

                // config
                Properties consumerConfig = getConfigKafka(bootstrapServers, Map.of());

                initKafkaTopic(consumerConfig, outputTopic);

                // Posting Instruction topic
                convertToPostingInstruction(groupId, consumerConfig, env, tableEnv);

                // Account Balance topic
                convertToAccountBalance(groupId, consumerConfig, env, tableEnv);


                Table joinTable = tableEnv.sqlQuery(QUERY_OUTPUT);

                KafkaUtils.SinkKafka(joinTable, outputTopic, schemaRegistryUrl, bootstrapServers, VKDepositTnxSchema.schema());

                env.execute("FlinkApp_vk_deposit_tnx" + developingEnv);

        }

        private static void convertToPostingInstruction(String groupId, Properties consumerConfig, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
                KafkaSource<Row> kafkaPostingInstruction = KafkaUtils.consumeKafka(PostingVkDepositTnxMapping.class, RAW_TOPIC_POSTING_INSTRUCTION, groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                DataStream<Row> sourcePostingInstruction = env.fromSource(kafkaPostingInstruction, WatermarkStrategy.noWatermarks(), RAW_TOPIC_POSTING_INSTRUCTION);

                Table inputTable = tableEnv.fromDataStream(sourcePostingInstruction, PostingInstructionTnxSchema.schema());

                tableEnv.createTemporaryView("POSTING_INSTRUCTION", inputTable);
        }

        private static void convertToAccountBalance(String groupId, Properties consumerConfig, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

                KafkaSource<Row> kafkaAccountBalance = KafkaUtils.consumeKafka(AccountBalanceVkDepositTnxMapping.class, RAW_TOPIC_ACCOUNT_BALANCE, groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                DataStream<Row> sourceAccountBalance = env.fromSource(kafkaAccountBalance, WatermarkStrategy.noWatermarks(), RAW_TOPIC_ACCOUNT_BALANCE);

                Table inputTable = tableEnv.fromDataStream(sourceAccountBalance, AccountBalanceTnxSchema.schema());

                tableEnv.createTemporaryView("ACCOUNT_BALANCE", inputTable);
        }
}