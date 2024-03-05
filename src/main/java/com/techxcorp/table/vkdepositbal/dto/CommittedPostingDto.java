package com.nhannt22.table.vkdepositbal.dto;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CommittedPostingDto {
    private boolean credit;
    private BigDecimal amount;
    private String denomination;

    @SerializedName("account_id")
    private String accountId;

    @SerializedName("account_address")
    private String accountAddress;
    private String asset;
    private String phase;
}
