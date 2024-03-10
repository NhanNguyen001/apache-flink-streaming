package com.nhannt22.learning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingCars_v7 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> carStream =
                env.readTextFile("src/main/resources/USA_cars.csv");

        DataStream<String> filteredStream = carStream.filter(
                (FilterFunction<String>) line ->
                        !line.contains("brand,model,year,price,mileage"));

        DataStream<Tuple2<String, Integer>> carDetails = filteredStream.
                map(new MapFunction<String, Tuple2<String, Integer>>() {
                    public Tuple2<String, Integer> map(String row)
                            throws Exception {
                        String[] fields = row.split(",");

                        return new Tuple2<>(fields[0], Integer.parseInt(fields[3]));
                    }
                });


        KeyedStream<Tuple2<String, Integer>, String> keyedCarStream =
                carDetails.keyBy(value -> value.f0);

        DataStream<Tuple2<String, Integer>> minPriceStream =
                keyedCarStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> minPriceSoFar,
                                                          Tuple2<String, Integer> currentPrice)
                            throws Exception {
                        if (currentPrice.f1 < minPriceSoFar.f1) {
                            return currentPrice;
                        } else {
                            return minPriceSoFar;
                        }
                    }
                });

        minPriceStream.print();

        env.execute("Streaming cars");
    }
}