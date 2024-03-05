package com.techxcorp.SerDes;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.Schema;

import com.google.gson.JsonElement;

public interface MappingFunctionJson {
    public List<Row> apply(JsonElement input, long timestamp);
    public TypeInformation<Row> getProducedType();
    public static Schema schemaOverride(){return null;};
}
