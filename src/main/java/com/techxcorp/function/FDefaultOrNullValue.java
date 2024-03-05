package com.techxcorp.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.AggregateFunction;

public class FDefaultOrNullValue extends AggregateFunction<String, FDefaultOrNullValue.PayloadAccumulator> {
    @Override
    public String getValue(PayloadAccumulator accumulator) {
        return ObjectUtils.defaultIfNull(accumulator.getData(), null);
    }

    @Override
    public PayloadAccumulator createAccumulator() {
        return new PayloadAccumulator();
    }

    public void accumulate(FDefaultOrNullValue.PayloadAccumulator accumulator, String value) {
        if (StringUtils.isNotEmpty(value)){
            accumulator.setData(value);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PayloadAccumulator {
        public String data;
    }

}
