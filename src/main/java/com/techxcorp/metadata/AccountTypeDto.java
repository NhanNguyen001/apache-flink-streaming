package com.techxcorp.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountTypeDto {
    private String accountType;
    private String accountTypeDesc;
}
