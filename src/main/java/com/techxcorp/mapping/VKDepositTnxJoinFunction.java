package com.nhannt22.mapping;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class VKDepositTnxJoinFunction extends CoProcessFunction<Row, Row, Row> {
    private ValueState<HashMap<String, Row>> accountState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // TODO Auto-generated method stub
        ValueStateDescriptor<HashMap<String, Row>> descriptor = new ValueStateDescriptor<HashMap<String, Row>>(
                "deposit-tnx-state", TypeInformation.of(new TypeHint<HashMap<String, Row>>() {
                }));
        accountState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement1(Row value, CoProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        // TODO Auto-generated method stub
        HashMap<String, Row> state = accountState.value();
        if (state == null) {
            state = new HashMap<>();
        }
        List<Row> rows = new ArrayList<>();
        
    }

    @Override
    public void processElement2(Row value, CoProcessFunction<Row, Row, Row>.Context ctx, Collector<Row> out)
            throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'processElement2'");
    }

}
