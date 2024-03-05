package com.techxcorp.constant;

/**
 * Define topic listen raw data tm
 */
public interface RawTopicConstant {
    /**
     * Topic mapping for vk_deposit_bal
     */
    String RAW_TOPIC_ACCOUNT_BALANCE = "raw_tm.vault.core_api.v1.balances.account_balance.events";
    String RAW_TOPIC_POSTING_INSTRUCTION = "raw_tm.vault.api.v1.postings.posting_instruction_batch.created";
    String RAW_TOPIC_ACCOUNT_CREATED = "raw_tm.vault.api.v1.accounts.account.created";
}
