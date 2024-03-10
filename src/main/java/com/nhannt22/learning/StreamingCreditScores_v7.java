package com.nhannt22.learning;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class StreamingCreditScores_v7 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<String> recordsStream =
                env.readTextFile("/Users/hdb3/Working/FinX/CODE/apache-flink-streaming/src/main/resources/data_example/credit.csv");

        DataStream<CreditRecord> creditStream = recordsStream
                .filter((FilterFunction<String>) line -> !line.contains(
                        "ID,LoanStatus,LoanAmount,Term,CreditScore,AnnualIncome,Home,CreditBalance"))
                .map(new MapFunction<String, CreditRecord>() {

                    @Override
                    public CreditRecord map(String s) throws Exception {

                        String[] fields = s.split(",");

                        return new CreditRecord(fields[0], fields[1], fields[2],
                                fields[3], fields[4], fields[5],
                                fields[6], fields[7]);
                    }
                });


        Table creditDetailsTable = tableEnv.fromDataStream(creditStream);

        Table resultsTable = creditDetailsTable.select($("id"), $("annualIncome"), $("home"));

        TableResult result = resultsTable.execute();

        result.print();
    }
}
