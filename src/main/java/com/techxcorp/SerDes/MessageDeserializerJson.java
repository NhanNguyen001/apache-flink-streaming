package com.techxcorp.SerDes;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

public class MessageDeserializerJson<E extends MappingFunctionJson> implements KafkaRecordDeserializationSchema<Row> {
    private transient Gson gson;
    private final Class<E> functionClass;
    private transient E function;

    public MessageDeserializerJson(Class<E> clazz) {
        this.functionClass = clazz;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.gson = new Gson();
        function = functionClass.getDeclaredConstructor().newInstance();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        try {
            
            return functionClass.getDeclaredConstructor().newInstance().getProducedType();
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
                | NoSuchMethodException | SecurityException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out)
            throws IOException {
        String jsonString = new String(record.value(), StandardCharsets.UTF_8);
        JsonElement element = gson.fromJson (jsonString, JsonElement.class);
        final List<Row> rows = function.apply(element, record.timestamp());
        for (Row row : rows) out.collect(row);
    }
}