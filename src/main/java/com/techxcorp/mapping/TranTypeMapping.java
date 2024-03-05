package com.techxcorp.mapping;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class TranTypeMapping {
    JsonArray jsonArray;

    public String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }

    public String readFileContent(ClassLoader classLoader, String fileName) {
        String content = "";
        try (InputStream inputStream = classLoader.getResourceAsStream(fileName)) {
            if (inputStream != null) {
                // Use BufferedReader to read the file as a string
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    StringBuilder stringBuilder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        stringBuilder.append(line).append("\n");
                    }

                    content = stringBuilder.toString();
                }
            } else {
                System.err.println("File not found: " + fileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    public TranTypeMapping() {
        Gson gson = new Gson();
        String content = readFileContent(TranTypeMapping.class.getClassLoader(), "reporting_data.json");
        JsonElement mappingDataRaw = gson.fromJson(content, JsonElement.class);
        jsonArray = mappingDataRaw.getAsJsonArray();
    }

    public Boolean checkContainCode(String code) {
        for (JsonElement jsonElement : jsonArray) {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            String codeMapping = jsonObject.get("code").getAsString();
            if (codeMapping.equals(code)) {
                return true;
            }
        }
        return false;
    }

    public String getTranType(String debitCreditInd, String code) {
        for (JsonElement jsonElement : jsonArray) {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            String codeMapping = jsonObject.get("code").getAsString();
            String debitTranTypeMapping = jsonObject.get("debit_tran_type").getAsString();
            String creditTranTypeMapping = jsonObject.get("credit_tran_type").getAsString();
            if (codeMapping.equals(code)) {
                if (debitCreditInd.equals("DR")) {
                    return debitTranTypeMapping;
                } else if (debitCreditInd.equals("CR")) {
                    return creditTranTypeMapping;
                }
            }
        }
        return null;
    }

    public String getGLcode(String debitCreditInd, String code) {
        for (JsonElement jsonElement : jsonArray) {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            String codeMapping = jsonObject.get("code").getAsString();
            String debitGlCode = jsonObject.get("debit_gl_code").getAsString();
            String creditGlCode = jsonObject.get("credit_gl_code").getAsString();
            if (codeMapping.equals(code)) {
                if (debitCreditInd.equals("DR")) {
                    return debitGlCode;
                } else if (debitCreditInd.equals("CR")) {
                    return creditGlCode;
                }
            }
        }
        return null;
    }

    public String getSourceType(String code) {
        for (JsonElement jsonElement : jsonArray) {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            String codeMapping = jsonObject.get("code").getAsString();
            String sourceType = jsonObject.get("source_type").getAsString();
            if (codeMapping.equals(code)) {
                return sourceType;
            }
        }
        return null;
    }

    public static void main(String[] args) {
    TranTypeMapping tranTypeMapping = new TranTypeMapping();
    System.out.println(tranTypeMapping.getGLcode("CR","ACMTMDOPOTHR"));

    }
}
