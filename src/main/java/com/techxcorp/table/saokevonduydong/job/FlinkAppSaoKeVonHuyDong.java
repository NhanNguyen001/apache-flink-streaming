package com.techxcorp.table.saokevonduydong.job;

import com.techxcorp.exception.FlinkException;
import com.techxcorp.table.saokevonduydong.mapping.AccountCreatedMapping;
import com.techxcorp.table.saokevonduydong.schema.AccountCreatedSchema;
import com.techxcorp.table.saokevonduydong.schema.SaokeVonHuyDongSchema;
import com.techxcorp.utils.KafkaUtils;
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

import static com.techxcorp.constant.RawTopicConstant.RAW_TOPIC_ACCOUNT_CREATED;
import static com.techxcorp.utils.FileUtils.readFilePropertiers;
import static com.techxcorp.utils.KafkaUtils.getConfigKafka;
import static com.techxcorp.utils.KafkaUtils.initKafkaTopic;
import static java.lang.String.format;

public class FlinkAppSaoKeVonHuyDong {

        public static final String QUERY_OUTPUT = "SELECT " +
                " SYM_RUN_DATE, " +
                " MA_LOAI, " +
                " TEN_CN, " +
                " THI_TRUONG, " +
                " KYHAN_SO, " +
                " LAISUAT_DCV, " +
                " ACCT_NO, " +
                " PERIOD_NUMBER, " +
                " PERIOD_TYPE, " +
                " LOAI_KYHAN, " +
                " CLIENT_NO, " +
                " ID, " +
                " NOW() AS SYNC_DATETIME_PROCCES, " +
                " ACCT_OPEN_DATE, " +
                " ACCT_MATURITY_DATE, " +
                " BRANCH_NO, " +
                " LAISUAT, " +
                " NGAY_MOSO " +
                "FROM ACCOUNT_CREATED";

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
                        String outputTopic = props.getProperty("vk.saoke.huydong.topic");
                        String groupId = props.getProperty("vk.saoke.huydong.group.id");

                        // config
                        Properties consumerConfig = getConfigKafka(bootstrapServers, Map.of());

                        initKafkaTopic(consumerConfig, outputTopic);

                        convertToAccountCreated(groupId, consumerConfig, env, tableEnv);

                        Table joinTable = tableEnv
                                .sqlQuery(QUERY_OUTPUT);

                        KafkaUtils.SinkKafka(joinTable, outputTopic, schemaRegistryUrl, bootstrapServers, SaokeVonHuyDongSchema.schema());

                        env.execute("FlinkApp_vk_saoke_huydong_" + developingEnv);
                }catch (Exception exception){
                        throw new FlinkException("Execute job FlinkApp_saoke_huydong_" + format("%s failed %s",developingEnv, exception.getMessage()));
                }


        }

        public static void convertToAccountCreated(String groupId, Properties consumerConfig, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
                KafkaSource<Row> kafkaAccountCreated = KafkaUtils.consumeKafka(
                        AccountCreatedMapping.class,
                        RAW_TOPIC_ACCOUNT_CREATED,
                        groupId, consumerConfig, OffsetResetStrategy.EARLIEST);

                DataStream<Row> sourceAccountCreated = env.fromSource(kafkaAccountCreated,
                        WatermarkStrategy.noWatermarks(),
                        RAW_TOPIC_ACCOUNT_CREATED);

                Table inputTableAccountCreated = tableEnv.fromDataStream(sourceAccountCreated,
                        AccountCreatedSchema.schema());

                tableEnv.createTemporaryView("ACCOUNT_CREATED", inputTableAccountCreated);
        }

        public static void main(String [] args) {
                start("sit");
        }
}