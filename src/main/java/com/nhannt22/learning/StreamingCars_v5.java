package com.nhannt22.learning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingCars_v5 {

    public static class Car {

        public String brand;
        public String model;
        public String year;
        public Integer price;
        public Integer miles;

        public Car() {
        }

        public Car(String brand, String model, String year, Integer price, Integer miles) {
            this.brand = brand;
            this.model = model;
            this.year = year;
            this.price = price;
            this.miles = miles;
        }

        @Override
        public String toString() {
            return brand + ", " + model + ", " + year + ", " + price + ", " + miles;
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> carStream =
                env.readTextFile("src/main/resources/USA_cars.csv");

        DataStream<String> filteredStream = carStream.filter(
                (FilterFunction<String>) line ->
                        !line.contains("brand,model,year,price,mileage"));

        DataStream<Car> carDetails = filteredStream.map(
                new MapFunction<String, Car>() {
                    public Car map(String row) throws Exception {
                        String[] fields = row.split(",");

                        return new Car(fields[0], fields[1], fields[2],
                                Integer.parseInt(fields[3]), Integer.parseInt(fields[4]));
                    }
                });


        KeyedStream<Car, String> keyedCarStream =
                carDetails.keyBy(car -> car.brand + car.model);

        DataStream<Car> maxPriceStream =
                keyedCarStream.maxBy("price");

        maxPriceStream.print();

        env.execute("Streaming cars");
    }
}