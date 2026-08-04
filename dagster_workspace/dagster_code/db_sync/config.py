# count 連續穩定多久才視為到齊(秒),依對方批次寫入速度調
DB_SYNC_STABLE_SECONDS = 300
# 超過這個時間 count 還在變就發警告
DB_SYNC_STALL_WARN_SECONDS = 7200


DB_SYNC_MAPPING = {
    "T_ACCOUNT_ALERT": {
        "freq": "daily",                                       # daily / monthly
        "source_db": "DDLQRMSV",
        "source_table": "dbo.T_ACCOUNT_ALERT",
        "bcp_target": "T_ACCOUNT_ALERT",                       # 我方目標表,省略則同 key
        "decrypt_fields": ["ACCOUNT", "ID", "BIRTHDATE"],      # 來源端密文,用 F_DecryptData 解
        "encrypt_fields": ["ACCOUNT", "ID"],                   # 進我方 DB 前用我們的方式加密
        "decrypt_key_env": "SOURCE_DECRYPT_KEY",               # VM1 .env 裡的變數名
        "sensor_watch_col": "NOTIFY_DATE",                     # sensor 判斷批次到齊的欄位(預設 NOTIFY_DATE)
        # "notify_date_col": "NOTIFY_DATE",                    # extract 的 WHERE 欄位,省略則同 sensor_watch_col
        "retries": 0,
        "retry_delay_sec": 60,
        "archive_dir": "T_ACCOUNT_ALERT_archive",
    },
}