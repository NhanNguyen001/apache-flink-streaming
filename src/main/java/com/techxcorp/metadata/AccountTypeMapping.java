package com.techxcorp.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.techxcorp.utils.FileUtils;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.techxcorp.utils.JsonUtils.fromJsonArray;

public class AccountTypeMapping {
    private final List<AccountTypeDto> accountTypes;


    public AccountTypeMapping() {
        String content = FileUtils.readFile("metadata/account_type/account_type.json", StandardCharsets.UTF_8);
        TypeReference<List<AccountTypeDto>> typeReference = new TypeReference<>() {};

        accountTypes  = fromJsonArray(content, typeReference);
    }

    public String getAccountTypeDescByType(String accountType) {
        return accountTypes.stream().filter(dto -> dto.getAccountType().equals(accountType))
                .map(AccountTypeDto::getAccountTypeDesc)
                .findFirst()
                .orElseGet(() -> "");
    }
}
