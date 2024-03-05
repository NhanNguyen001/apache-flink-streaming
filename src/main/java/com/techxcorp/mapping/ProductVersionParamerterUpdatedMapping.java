package com.techxcorp.mapping;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
// import java.util.UUID;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.techxcorp.SerDes.MappingFunctionJson;

public class ProductVersionParamerterUpdatedMapping implements MappingFunctionJson {
        // raw_tm.vault.api.v1.products.product_version.parameter.updated
        public static Schema schemaOverride() {
                return Schema.newBuilder()
                                .column("PRODUCT_ID", DataTypes.STRING())
                                .column("PARAM_NAME", DataTypes.STRING())
                                .column("PARAM_VALUE", DataTypes.DECIMAL(22, 5))
                                .column("PROC_TIME", DataTypes.TIMESTAMP(3))
                                .build();
        };

        public String getDataString(JsonObject jsonObject, String fieldName) {
                return jsonObject.has(fieldName)
                                ? jsonObject.get(fieldName).getAsString()
                                : "";
        }

        public BigDecimal getDataNumber(JsonObject jsonObject, String fieldName) {
                return jsonObject.has(fieldName)
                                ? jsonObject.get(fieldName).getAsBigDecimal()
                                : BigDecimal.ZERO;
        }

        @Override
        public List<Row> apply(JsonElement input, long kafkaTimestamp) {
                List<Row> rows = new ArrayList<>();
                JsonObject jsonObj = input.getAsJsonObject();
                JsonObject paramObj = jsonObj.get("param").getAsJsonObject();
                final Row row = Row.withNames(RowKind.INSERT);
                row.setField("PRODUCT_ID", getDataString(jsonObj, "id"));
                String paramName = getDataString(paramObj, "name");
                if (paramName.equalsIgnoreCase("deposit_interest_rate")) {
                        row.setField("PARAM_NAME", paramName);
                        row.setField("PARAM_VALUE", getDataNumber(paramObj, "value"));
                }
                row.setField("PROC_TIME", LocalDateTime.now());
                rows.add(row);

                return rows;
        }

        @Override
        public TypeInformation<Row> getProducedType() {
                return Types.ROW_NAMED(
                                new String[] { "PRODUCT_ID", "PARAM_NAME", "PARAM_VALUE", "PROC_TIME" },
                                new TypeInformation[] { Types.STRING, Types.STRING, Types.BIG_DEC, Types.LOCAL_DATE_TIME });
        }
}
