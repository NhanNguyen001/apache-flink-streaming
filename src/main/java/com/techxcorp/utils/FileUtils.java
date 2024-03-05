package com.techxcorp.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.techxcorp.exception.FlinkException;
import com.techxcorp.metadata.InternalAccountDto;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static java.lang.String.format;

public class FileUtils {
    public static String readFile(String path, Charset encoding) {
        try {
           ;
            byte[] encoded =  Objects.requireNonNull(FileUtils.class.getClassLoader()
                    .getResourceAsStream(path)).readAllBytes();
            return new String(encoded, encoding);
        }catch (IOException ex) {
            throw new FlinkException(format("Read file %s error %s", path, ex));
        }

    }
    public static Properties readFilePropertiers(String developingEnv) {
        String fileName = developingEnv + "/app.conf";
        Properties props = new Properties();
        InputStream inputStream = FileUtils.class.getClassLoader()
                .getResourceAsStream(fileName);

        try {
            props.load(inputStream);
        } catch (IOException e) {
            throw new FlinkException(format("Read %s file config error %s", fileName, e.getMessage() ));
        }
        return props;
    }

    public static List<InternalAccountDto> readFileInternalAccount() {
        String internalAccounts  = readFile("metadata/account/internal_account.json", StandardCharsets.UTF_8);
        TypeReference<List<InternalAccountDto>> typeReference = new TypeReference<>() {};

        return JsonUtils.fromJsonArray(internalAccounts, typeReference);
    }
}
