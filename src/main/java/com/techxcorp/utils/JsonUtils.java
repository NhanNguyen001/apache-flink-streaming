package com.techxcorp.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
public class JsonUtils {
    private static final Gson gson = new Gson();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private JsonUtils(){
    }
    public static String getDataString(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null)
            return "";
        return jsonObject.has(fieldName)
                ? jsonObject.get(fieldName).getAsString()
                : "";
    }

    public static BigDecimal getDataNumber(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null)
            return null;
        return jsonObject.has(fieldName)
                ? jsonObject.get(fieldName).getAsBigDecimal()
                : null;
    }

    public static JsonObject getDataJsonObject(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null) {
            return null;
        }
        return jsonObject.has(fieldName) && !jsonObject.get(fieldName).isJsonNull()
                ? jsonObject.get(fieldName).getAsJsonObject()
                : null;
    }

    public static String concatBtcCode(JsonObject transactionCodeObject){
        String domain = getDataString(transactionCodeObject, "domain");
        String family = getDataString(transactionCodeObject, "family");
        String subfamily = getDataString(transactionCodeObject, "subfamily");

        return domain + family + subfamily;
    }

    // Convert JSON string to object
    public static <T> T fromJson(String json, Type clazz) {
        return gson.fromJson(json, clazz);
    }
    // Convert JsonElement to object
    public static <T> T fromJsonElement(JsonElement jsonElement, Class<T> clazz) {
        return gson.fromJson(jsonElement, clazz);
    }

    public static <T> T toArrayObject(JsonArray data) {
        Type listType = new TypeToken<List<?>>() {}.getType();
        return gson.fromJson(data, listType);
    }

    // Method to convert JSON string to a List of objects
    public static <T> List<T> fromJsonArray(String jsonArray, Class<T> valueType) throws IOException {
        return objectMapper.readValue(jsonArray, objectMapper.getTypeFactory().constructCollectionType(List.class, valueType));
    }

    // Method to convert an object to a JSON string
    public static String toJson(Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }

    // Method to convert JSON string to a List of objects using TypeReference
    public static <T> List<T> fromJsonArray(String jsonArray, TypeReference<List<T>> typeReference)  {
        try {
            return objectMapper.readValue(jsonArray, typeReference);
        }catch (IOException exception){
            return List.of();
        }

    }
    public static JsonElement parseJson(String json) {
        return JsonParser.parseString(json);
    }
}
