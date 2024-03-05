package com.techxcorp.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InternalAccountDto {
    private String acctId;
    private String acctType;
}
