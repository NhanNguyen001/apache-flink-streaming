package com.techxcorp.jobs;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.techxcorp.table.saokevonduydong.job.FlinkAppSaoKeVonHuyDong;
import com.techxcorp.table.vkdepositbal.job.FlinkAppVKDepositBalV2;
import com.techxcorp.table.vkdeposittnx.job.FlinkAppVKDepositTnxV2;

public class App {
    public static void main(String[] args) {
        try {
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            String table_destination = applicationProperties.get("Config").get("table").toString();
            String developingEnv = applicationProperties.get("Config").get("developingEnv").toString();
            if (table_destination.contains("vk_client")) {
                FlinkAppVKClient.start(developingEnv);
            } else if (table_destination.contains("vk_deposit_bal")) {
                FlinkAppVKDepositBalV2.start(developingEnv);
            } else if (table_destination.contains("vk_deposit_tnx")) {
                FlinkAppVKDepositTnxV2.start(developingEnv);
            } else if (table_destination.contains("vk_card_tnx")) {
                FlinkAppVKCardTnx.start(developingEnv);
            } else if(table_destination.contains("vk_card_dim")) {
                FlinkAppVKCardDim.start(developingEnv);
            } else if(table_destination.contains("flink_account_created")){
                FlinkAppVKAccountCreated.start(developingEnv);
            } else if(table_destination.contains("flink_product_info")){
                FlinkAppVKProductInfo.start(developingEnv);
            } else if(table_destination.contains("vk_saoke_huydong")){
                FlinkAppSaoKeVonHuyDong.start(developingEnv);
            }
            else {
                System.out.println("No table destination found");
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
