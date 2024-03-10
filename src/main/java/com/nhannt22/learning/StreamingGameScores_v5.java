package com.nhannt22.learning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class StreamingGameScores_v5 {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getConfig().setGlobalJobParameters(params);
//
//        if (!params.has("host") || !params.has("port")) {
//            System.out.println("Please specify values for --host and --port");
//
//            System.exit(1);
//            return;
//        }

        DataStream<String> dataStream = env.socketTextStream(
                "localhost",
                9000);

        DataStream<Tuple3<String, String, Integer>> gameScores = dataStream
                .flatMap(new LevelScoreExtractorFn())
                .filter(new FilterPlayersAboveThresholdFn(100));
        gameScores.print();

        env.execute("Streaming game scores");
    }

    public static class LevelScoreExtractorFn implements
            FlatMapFunction<String, Tuple3<String, String, Integer>> {

        private static final Map<Integer, String> levelLookup = new HashMap<>();

        static {
            levelLookup.put(1, "Level 1");
            levelLookup.put(2, "Level 2");
            levelLookup.put(3, "Level 3");
            levelLookup.put(4, "Level 4");
        }

        public void flatMap(String row, Collector<Tuple3<String, String, Integer>> out)
                throws Exception {

            String[] tokens = row.split(",");

            if (tokens.length < 2) {
                return;
            }

            for (Integer indexKey : levelLookup.keySet()) {
                if (indexKey < tokens.length) {

                    out.collect(new Tuple3<String, String, Integer>(
                            tokens[0].trim(), levelLookup.get(indexKey),
                            Integer.parseInt(tokens[indexKey].trim())));

                    System.out.println("index key" + indexKey);
                    System.out.println(new Tuple3<String, String, Integer>(
                            tokens[0].trim(), levelLookup.get(indexKey),
                            Integer.parseInt(tokens[indexKey].trim())));

                }
            }
        }

    }

    public static class FilterPlayersAboveThresholdFn implements
            FilterFunction<Tuple3<String, String, Integer>> {

        private int scoreThreshold = 0;

        public FilterPlayersAboveThresholdFn(int scoreThreshold) {
            this.scoreThreshold = scoreThreshold;
        }

        @Override
        public boolean filter(Tuple3<String, String, Integer> playersLevelScores) throws Exception {
            return playersLevelScores.f2 > scoreThreshold;
        }
    }
}