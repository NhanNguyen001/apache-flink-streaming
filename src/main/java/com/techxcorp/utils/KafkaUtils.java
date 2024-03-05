package com.nhannt22.utils;

import com.nhannt22.SerDes.MessageDeserializerJson;
import com.nhannt22.exception.FlinkException;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.String.format;

public class KafkaUtils {

    public static void initKafkaTopic(Properties consumerConfig, String topic) {
        // create topic
        try (AdminClient adminClient = AdminClient.create(consumerConfig)) {
            adminClient.createTopics(List.of(new NewTopic(topic, 5, (short) -1)));
            System.out.println("Topic created");
        } catch (Exception e) {
            throw new FlinkException(format("Create topic error %s", e.getMessage()));
        }
    }
    public static Properties getConfigKafka(String bootstrapServers, Map<String, String> additionalConfig) {
        Properties consumerConfig = new Properties();
        consumerConfig.put("security.protocol", "SASL_SSL");
        consumerConfig.put("sasl.mechanism", "AWS_MSK_IAM");
        consumerConfig.put("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
        consumerConfig.put("sasl.client.callback.handler.class",
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");
        consumerConfig.put("bootstrap.servers", bootstrapServers);
        consumerConfig.putAll(additionalConfig);
        return consumerConfig;
    }

    public static KafkaSource<Row> consumeKafka(Class mappingClass, String topic,
                                             String groupId, Properties consumerConfig, OffsetResetStrategy offsetResetStrategy) {
        return KafkaSource.<Row>builder()
                .setTopics(topic)
                .setGroupId(groupId)
                .setProperties(consumerConfig)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(offsetResetStrategy))
                .setDeserializer(new MessageDeserializerJson<>(mappingClass))
                .build();
    }

    public static void SinkKafka(Table inputTable, String sinkTopic, String schemaRegistryUrl,
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
                .option("value.format", "avro-confluent")
                .option("value.avro-confluent.url", schemaRegistryUrl)
                .option("value.fields-include", "ALL")
                .option("properties.bootstrap.servers", bootstrapServers)
                .build());
        result.print();
    }
}
