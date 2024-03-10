package com.nhannt22.learning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingCars_v2 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> carStream =
                env.readTextFile("src/main/resources/USA_cars.csv");

        DataStream<String> filteredStream = carStream.filter(
                (FilterFunction<String>) line ->
                        !line.contains("brand,model,year,price,mileage"));

        DataStream<Tuple5<String, String, String, Integer, Integer>> carDetails = filteredStream.map(
                new MapFunction<String, Tuple5<String, String, String, Integer, Integer>>() {
                    public Tuple5<String, String, String, Integer, Integer> map(String row)
                            throws Exception {
                        String[] fields = row.split(",");

                        return new Tuple5<>(fields[0], fields[1], fields[2],
                                Integer.parseInt(fields[3]), Integer.parseInt(fields[4]));
                    }
                });


        KeyedStream<Tuple5<String, String, String, Integer, Integer>, String> keyedCarStream =
                carDetails.keyBy(value -> value.f0 + value.f1);

        DataStream<Tuple5<String, String, String, Integer, Integer>> minYearStream =
                keyedCarStream.min(2);

        minYearStream.project(0, 1, 2, 3).print();

        env.execute("Streaming cars");
    }
}