package com.techxcorp.table.saokevonduydong.dto;

import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class InstanceParamValsDto {

    @SerializedName("account_type")
    private String accountType;

    @SerializedName("annual_interest_rate")
    private BigDecimal annualInterestRate;

    @SerializedName("auto_renewal_type")
    private String autoRenewalType;

    @SerializedName("deposit_account")
    private String depositAccount;

    @SerializedName("deposit_start_datetime")
    private String depositStartDatetime;

    private String principal;

    private String status;

    @SerializedName("tenor_amount")
    private Integer tenorAmount;

    @SerializedName("tenor_unit")
    private String tenorUnit;
}
