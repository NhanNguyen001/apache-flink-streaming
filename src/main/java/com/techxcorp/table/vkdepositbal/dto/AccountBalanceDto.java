package com.techxcorp.table.vkdepositbal.dto;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AccountBalanceDto {
    private String id;

    @SerializedName("account_id")
    private String accountId;

    @SerializedName("account_address")
    private String accountAddress;
    private String phase;
    private String asset;
    private String denomination;

    @SerializedName("posting_instruction_batch_id")
    private String postingInstructionBatchId;

    @SerializedName("update_posting_instruction_batch_id")
    private String updatePostingInstructionBatchId;

    @SerializedName("value_time")
    private String valueTime;


    private BigDecimal amount;

    @SerializedName("total_debit")
    private BigDecimal totalDebit;

    @SerializedName("total_credit")
    private BigDecimal totalCredit;
}
