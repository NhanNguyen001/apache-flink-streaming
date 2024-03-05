# Deploy Flink App
Các config của từng môi trường tại resource/${env}/app.conf:
- bootstrap.servers
- schema.registry.url


Setup môi trường deploy tại file [list_app.conf](list_app.conf)
- APPLICATION_NAME_LIST: danh sách các application cần deploy, ngăn cách nhau bằng dấu ","
    Tên application theo cấu trúc: FlinkApp_tên_bảng
    - Danh sách các bảng: 
        - vk_client
        - vk_card_tnx
        - vk_card_dim
        - vk_deposit_tnx
        - vk_deposit_bal
        - flink_account_created
        - flink_product_info    
- DEV_ENV: môi trường cần deploy
    - Danh sách các môi trường:
        - sit
        - uat


*Note: Trong các trường hợp update câu query, cần xóa application và deploy lại

# Rerun Flink App
- update tên app tại [list_app.conf](list_app.conf)
- nếu cần consume lại dữ liệu, update Kafka group id tại resources/${dev_env}/app.conf

