package com.techxcorp.jobs;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.Map;

import com.techxcorp.mapping.TranTypeMapping;
public class AppTest {

    public  String readFile(String path, Charset encoding) throws IOException
    {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
    public String getDataString(JsonObject jsonObject, String fieldName) {
        if(jsonObject == null)
            return null;
        return jsonObject.has(fieldName) && !jsonObject.get(fieldName).isJsonNull()
                ? jsonObject.get(fieldName).getAsString()
                : "";
    }

    public BigDecimal getDataNumber(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null)
            return null;
        return jsonObject.has(fieldName) && !jsonObject.isJsonNull()
                ? jsonObject.get(fieldName).getAsBigDecimal()
                : BigDecimal.ZERO;
    }

    public JsonObject getDataJsonObject(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null){
            return null;
        }
        return jsonObject.has(fieldName) && !jsonObject.get(fieldName).isJsonNull()
                ? jsonObject.get(fieldName).getAsJsonObject()
                : null;
    }


    @Test
    public void testApp() {
        String filePath = "src/main/resources/raw_tm.vault.api.v1.postings.posting_instruction_batch.created.json";
        String content = null;
        try {
            content = readFile(filePath, StandardCharsets.UTF_8);
            Gson gson = new Gson(); // Creates new instance of Gson
            JsonElement element = gson.fromJson (content, JsonElement.class); //Converts the json string to JsonElement without POJO
            JsonObject jsonObj = element.getAsJsonObject(); //Converting JsonElement to JsonObject
            JsonObject postingInstructionBatch = jsonObj.getAsJsonObject("posting_instruction_batch");
            JsonArray postingInstruction = postingInstructionBatch.getAsJsonArray("posting_instructions");
            Map<String, String> map = new HashMap<String, String>();
            int numberOfElements = postingInstruction.size();
            JsonObject batchDetailsObject = getDataJsonObject(postingInstructionBatch, "batch_details");
            String transTypeName = getDataString(batchDetailsObject, "trans_type_name");

            for(int i=0; i<numberOfElements; i++) {
                JsonObject postingInstructionObject = postingInstruction.get(i).getAsJsonObject();
                JsonObject transactionCodeObject = getDataJsonObject(postingInstructionObject, "transaction_code");
                String domain = getDataString(transactionCodeObject, "domain");
                String family  = getDataString(transactionCodeObject, "family");
                String subfamily = getDataString(transactionCodeObject, "subfamily");
                String code = domain+family+subfamily;
                System.out.println("code: "+code);
                TranTypeMapping tranTypeMapping = new TranTypeMapping();
                String tranType = tranTypeMapping.getTranType("DR", code);
                System.out.println("tranType: "+tranType);
                System.out.println(transactionCodeObject == null);
            }





        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
