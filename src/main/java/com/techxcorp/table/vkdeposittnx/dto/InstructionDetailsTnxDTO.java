package com.nhannt22.table.vkdeposittnx.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.ZonedDateTime;
@Data
@NoArgsConstructor
@AllArgsConstructor
public class InstructionDetailsTnxDTO {
    private String xRequestId;
    private String xServiceName;
    private String authCode;
    private String bankCode;
    private String bankingProduct;
    private String beneficiaryPartyId;
    private String channel;
    private ZonedDateTime createdDate;
    private String currency;
    private boolean forcePost;
    private String fromAccount;
    private boolean isFee;
    private String narrative;
    private String receiverName;
    private String referenceNumber;
    private String rtfPostingType;
    private String rtfType;
    private String senderName;
    private String toAccount;
    private String transTypeName;
    private String transactionCode;

    private ZonedDateTime transmissionDatetime;

}
