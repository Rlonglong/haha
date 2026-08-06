TABLE_CSV_MAPPING = {
    # "TEMPLATE_TABLE": {
    #     # ==========================================
    #     # 0. FTP 下載設定
    #     # ==========================================
    #     "use_ftp_fetch": True,                              # [布林值] 是否啟用自動下載檔案。(沒填預設: False)
    #     "ftp_remote_dir": "/data/fromFPP/TEMPLATE/",        # [字串] FTP 來源目錄路徑。(沒填預設: 無。若 use_ftp_fetch 為 True，此項與 prefix 必填，或改填 template)
    #     "ftp_remote_filename_prefix": "TEMPLATE_{date}",    # [字串] FTP 檔案前綴，{date} 自動替換。(沒填預設: 無)
    #     
    #     # ZIP 解壓縮相關設定
    #     "use_ftp_zip": True,                                # [布林值] 來源是否為 ZIP 檔，需解壓縮。(沒填預設: False)
    #     "zip_inner_filename": "EXNP_TEMPLATE",              # [字串] ZIP 內要抽出的目標檔名。(沒填預設: None，系統會自動抓 ZIP 內的第一個檔案)
    #     
    #     # 本地暫存與備份設定
    #     "staging_subfolder": "TEMPLATE",                    # [字串] staging 暫存區的子資料夾名。(沒填預設: 自動帶入 Table Name)
    #     "archive_dir": "/run/media/root/D/data/archive/",   # [字串] 處理完畢後的封存備份路徑。(沒填預設: 無。若有開 FTP 下載則必填)
    #
    #     # ==========================================
    #     # 1. 檔案路徑與基礎設定 (ETL 處理階段)
    #     # ==========================================
    #     "input_folder": "/run/media/root/D/data/fromFPP/",  # [字串] 要處理的原始檔讀取路徑。(沒填預設: 若為 FTP 模式預設同 Table Name，否則會報錯中斷)
    #     "output_folder": "/run/media/root/D/data/CLEANED/", # [字串] 清洗完畢後的 CSV 輸出路徑。(沒填預設: 輸出至 input_folder 原目錄)
    #     "template": "TEMPLATE_{date}.csv",                  # [字串] 本地端標準檔名模板。(【必填】)
    #     "freq": "daily",                                    # [字串] 檔案到站頻率 "daily" 或 "monthly"。(沒填預設: "daily")
    #     "delimiter": "|",                                   # [字串] CSV 欄位分隔符號。(沒填預設: ",")
    #     "use_data_rule": True,                              # [布林值] 是否套用檔規切割固定寬度字元。(沒填預設: False)
    #
    #     # ==========================================
    #     # 2. 處理邏輯
    #     # ==========================================
    #     "has_clean_func": True,                             # [布林值] 是否有專屬客製化清洗函式。(沒填預設: False)
    #
    #     # ==========================================
    #     # 3. 容錯與重試機制
    #     # ==========================================
    #     "retries": 3,                                       # [整數] 失敗自動重試次數。(沒填預設: 0)
    #     "retry_delay_sec": 60,                              # [整數] 重試間隔時間，單位秒。(沒填預設: 60)
    #
    #     # ==========================================
    #     # 4. BCP (Bulk Copy Program) 匯入設定
    #     # ==========================================
    #     "bcp_target": "v_BCP_TEMPLATE",                     # [字串] BCP 最終匯入的目標表或 View 名稱。(沒填預設: 自動帶入 Table Name)
    #
    #     # ==========================================
    #     # 5. Index 管理 (效能優化)
    #     # ==========================================
    #     "use_index": True,                                  # [布林值] 是否先 Disable Index 再 Rebuild 以加速寫入。(沒填預設: False)
    #     "index_name": "IX_TEMPLATE",                        # [字串] 動態管理的 Index 名稱。(沒填預設: 自動帶入 IX_加上表名)
    #
    #     # ==========================================
    #     # 6. Partition (資料分割) 與生命週期管理
    #     # ==========================================
    #     "use_partition": True,                              # [布林值] 是否有使用 Partition Function 分割表。(沒填預設: False)
    #     "partition_function": "pf_Monthly",                 # [字串] 綁定的 Partition Function 名稱。(沒填預設: 無)
    #     "retention_years": 2,                               # [整數] 正式表資料保留年限。(沒填預設: 無，代表不自動封存淘汰)
    #     "archive_table": "TEMPLATE_Archive",                # [字串] 退役歷史資料移轉的存檔表名稱。(沒填預設: 無)
    #
    #     # ==========================================
    #     # 7. 資安與加密設定
    #     # ==========================================
    #     "encrypt_fields": ["ACT_NO", "ACCT_NBR_ORI"]        # [清單] 需雜湊(Hash)或加密的敏感欄位。留空 [] 代表不加密。(沒填預設: [])
    # }, 
    "T_CUST": {
        # ==========================================
        # 0. ftp 設定
        # ==========================================
        "use_ftp_fetch": True,
        "ftp_remote_dir": "/data/fromFPP/T_CUST/",
        "ftp_remote_filename_prefix": "T_CUST_{date}",
    
        # zip 相關設定
        "use_ftp_zip": True,
        # "zip_inner_filename": "EXNP_PSPDC55_PS",
        "expected_part_count": 15, 
    
        "staging_subfolder": "T_CUST",
        "archive_dir": "/run/media/root/D/data/archive/T_CUST",
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_CUST",
        "output_folder": "/run/media/root/D/data/T_CUST/T_CUST_CLEANED",
        "template": "T_CUST_{date}.csv",
        "freq": "monthly",
        "delimiter": "〨",
        "use_data_rule": True,
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        "bcp_target": "v_BCP_T_CUST",
        # ==========================================
        # 5. Index 管理
        # ==========================================
        "use_index": False,
        # "index_name": "IX_T_CUST",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        "use_partition": True,
        "partition_function": "pf_CUST_Monthly",
        "retention_years": 2,
        "archive_table": "T_CUST_Archive",
        # ==========================================
        # 7. 加密欄位（預設空清單 = 不加密）
        # ==========================================
        "encrypt_fields": ["ID"]
    },
    "T_CUST_PS": {
        # ==========================================
        # 0. ftp 設定
        # ==========================================
        "use_ftp_fetch": True,
        "ftp_remote_dir": "/data/fromFPP/T_CUST_PS/",
        "ftp_remote_filename_prefix": "T_CUST_PS_{date}",
    
        # zip 相關設定
        "use_ftp_zip": True,
        # "zip_inner_filename": "EXNP_PSPDC55_PS",
        "expected_part_count": 8, 
        "multi_part_start": "A",
    
        "staging_subfolder": "T_CUST_PS",
        "archive_dir": "/run/media/root/D/data/archive/T_CUST_PS",
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_CUST_PS",
        "output_folder": "/run/media/root/D/data/T_CUST_PS/T_CUST_PS_CLEANED",
        "template": "T_CUST_PS_{date}.csv",
        "freq": "monthly",
        "delimiter": "〨",
        "use_data_rule": True,
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        "bcp_target": "v_BCP_T_CUST_PS",
        # ==========================================
        # 5. Index 管理
        # ==========================================
        "use_index": False,
        # "index_name": "IX_T_CUST_PS",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        "use_partition": True,
        "partition_function": "pf_CUST_PS_Monthly",
        "retention_years": 2,
        "archive_table": "T_CUST_PS_Archive",
        # ==========================================
        # 7. 加密欄位（預設空清單 = 不加密）
        # ==========================================
        "encrypt_fields": ["ACT_NO", "ID"]
    },
    "T_LOW_INCOME_ACCT": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_LOW_INCOME_ACCT",
        "output_folder": "/run/media/root/D/data/T_LOW_INCOME_ACCT/T_LOW_INCOME_ACCT_CLEANED",
        "template": "T_LOW_INCOME_ACCT_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_LOW_INCOME_ACCT",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_TXN_PS": {
        # ==========================================
        # 0. ftp 設定
        # ==========================================
        "use_ftp_fetch": True,
        "ftp_remote_dir": "/data/fromFPP/T_TXN_PS/",
        "ftp_remote_filename_prefix": "T_TXN_PS_{date}",
    
        # zip 相關設定
        "use_ftp_zip": True,
        "zip_inner_filename": "EXNP_PSPDC55_PS",
    
        "staging_subfolder": "T_TXN_PS",
        "archive_dir": "/run/media/root/D/data/archive/T_TNX_PS",
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_TXN_PS",
        "output_folder": "/run/media/root/D/data/T_TXN_PS/T_TXN_PS_CLEANED",
        "template": "T_TXN_PS_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        "use_data_rule": True,
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        "bcp_target": "v_BCP_T_TXN_PS",
        # ==========================================
        # 5. Index 管理
        # ==========================================
        "use_index": True,
        "index_name": "IX_T_TXN_PS",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        "use_partition": True,
        "partition_function": "pf_TXN_Monthly",
        "retention_years": 2,
        "archive_table": "T_TXN_PS_Archive",
        # ==========================================
        # 7. 加密欄位（預設空清單 = 不加密）
        # ==========================================
        "encrypt_fields": ["ACT_NO", "ACCT_NBR_ORI", "OWN_TRANS_ACCT"]
    },
    "T_TXN_PS_FIRST": {
        # ==========================================
        # 0. ftp 設定
        # ==========================================
        "use_ftp_fetch": True,
        "ftp_remote_dir": "/data/fromFPP/T_TXN_PS_FIRST/",
        "ftp_remote_filename_prefix": "T_TXN_PS_FIRST_{date}",
    
        # zip 相關設定
        "use_ftp_zip": True,
    
        "staging_subfolder": "T_TXN_PS_FIRST",
        "archive_dir": "/run/media/root/D/data/archive/T_TNX_PS_FIRST",
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_TXN_PS_FIRST",
        "output_folder": "/run/media/root/D/data/T_TXN_PS_FIRST/T_TXN_PS_FIRST_CLEANED",
        "template": "T_TXN_PS_FIRST_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        "use_data_rule": True,
        "data_rule_sheet": "T_TXN_PS",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        "bcp_target": "v_BCP_T_TXN_PS_FIRST",
        # ==========================================
        # 5. Index 管理
        # ==========================================
        "use_index": True,
        "index_name": "IX_T_TXN_PS_FIRST",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        "use_partition": True,
        "partition_function": "pf_TXN_FIRST_Monthly",
        "retention_years": 2,
        "archive_table": "T_TXN_PS_FIRST_Archive",
        # ==========================================
        # 7. 加密欄位（預設空清單 = 不加密）
        # ==========================================
        "encrypt_fields": ["ACT_NO", "ACCT_NBR_ORI", "OWN_TRANS_ACCT"]
    },
    "T_EARLYJOB_LIST": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_EARLYJOB_LIST",
        "output_folder": "/run/media/root/D/data/T_EARLYJOB_LIST/T_EARLYJOB_LIST_CLEANED",
        "template": "T_EARLYJOB_LIST_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_EARLYJOB_LIST",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_CUST_PS_CHG": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_CUST_PS_CHG",
        "output_folder": "/run/media/root/D/data/T_CUST_PS_CHG/T_CUST_PS_CHG_CLEANED",
        "template": "T_CUST_PS_CHG_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_CUST_PS_CHG",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_MAX_WITHDRAWALS_FOR_3DS_SAVING": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_MAX_WITHDRAWALS_FOR_3DS_SAVING",
        "output_folder": "/run/media/root/D/data/T_MAX_WITHDRAWALS_FOR_3DS_SAVING/T_MAX_WITHDRAWALS_FOR_3DS_SAVING_CLEANED",
        "template": "T_MAX_WITHDRAWALS_FOR_3DS_SAVING_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_MAX_WITHDRAWALS_FOR_3DS_SAVING",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_FARPOST": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_FARPOST",
        "output_folder": "/run/media/root/D/data/T_FARPOST/T_FARPOST_CLEANED",
        "template": "T_FARPOST_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_FARPOST",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_SUBSIDY": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_SUBSIDY",
        "output_folder": "/run/media/root/D/data/T_SUBSIDY/T_SUBSIDY_CLEANED",
        "template": "T_SUBSIDY_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_SUBSIDY",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_LOW_INCOME": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_LOW_INCOME",
        "output_folder": "/run/media/root/D/data/T_LOW_INCOME/T_LOW_INCOME_CLEANED",
        "template": "T_LOW_INCOME_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_LOW_INCOME",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_CUST_PS_LASTCTRL": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_CUST_PS_LASTCTRL",
        "output_folder": "/run/media/root/D/data/T_CUST_PS_LASTCTRL/T_CUST_PS_LASTCTRL_CLEANED",
        "template": "T_CUST_PS_LASTCTRL_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_CUST_PS_LASTCTRL",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_FRAUD360": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_FRAUD360",
        "output_folder": "/run/media/root/D/data/T_FRAUD360/T_FRAUD360_CLEANED",
        "template": "T_FRAUD360_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_FRAUD360",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "T_CUST_WARNINGFLG": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/T_CUST_WARNINGFLG",
        "output_folder": "/run/media/root/D/data/T_CUST_WARNINGFLG/T_CUST_WARNINGFLG_CLEANED",
        "template": "T_CUST_WARNINGFLG_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_T_CUST_WARNINGFLG",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
    "WARNINGFLG_BONUS": {
        # ==========================================
        # 1. 檔案路徑與基礎設定
        # ==========================================
        "input_folder": "/run/media/root/D/data/fromFPP/WARNINGFLG_BONUS",
        "output_folder": "/run/media/root/D/data/WARNINGFLG_BONUS/WARNINGFLG_BONUS_CLEANED",
        "template": "WARNINGFLG_BONUS_{date}.csv",
        "freq": "daily",
        "delimiter": "|",
        # ==========================================
        # 2. 處理邏輯
        # ==========================================
        # "has_clean_func": True,
        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,
        # ==========================================
        # 4. BCP 目標
        # ==========================================
        # "bcp_target": None,
        # ==========================================
        # 5. Index 管理
        # ==========================================
        # "use_index": False,
        # "index_name": "IX_WARNINGFLG_BONUS",
        # ==========================================
        # 6. Partition 管理
        # ==========================================
        # "use_partition": False,
        # "partition_function": "",
        # "retention_years": None,
        # "archive_table": "",
    },
}


SQL_TO_CSV_MAPPING = {
    "mrt_ATM_C2_earlyjob_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_ATM_C2_earlyjob",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/02_atm3_warning_list/",
        # "ftp_remote_path": "/data/PostBS/Project_output/02_atm3_warning_list/",
        "template": "ATM_C2_earlyjob_EXPORT_{date}.csv",
        # "one_more_day_flg": True,
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",
        

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "ATM_C2_earlyjob",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },

    "mrt_ATM_E2_earlyjob_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_ATM_E2_earlyjob",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/13_withdrawal_across_days/",
        "template": "ATM_E2_earlyjob_EXPORT_{date}.csv",
        # "one_more_day_flg": True,
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "ATM_E2_earlyjob",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },

    "mrt_ATM_C2_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_ATM_C2",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/02_atm3_warning_list/",
        # "ftp_remote_path": "/data/PostBS/Project_output/02_atm3_warning_list/",
        "template": "ATM_C2_EXPORT_{date}.csv",
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",
        

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "ATM_C2",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },

    "mrt_ATM_E2_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_ATM_E2",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/13_withdrawal_across_days/",
        "template": "ATM_E2_EXPORT_{date}.csv",
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "ATM_E2",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },

    "mrt_ATM_H_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_ATM_H",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/19_atm_alert_h/",
        "template": "ATM_H_EXPORT_{date}.csv",
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "ATM_H",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },

    "mrt_VIRTUAL_TRANSACT_1_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_VIRTUAL_TRANSACT_1",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/06_virtual_transact_1/",
        "template": "VIRTUAL_TRANSACT_1_EXPORT_{date}.csv",
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "GSS20250514_Virtual_Txn_Control_List_1",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },

    "mrt_VIRTUAL_TRANSACT_2_EXPORT": {
        # ==========================================
        # 1. 資料來源設定（SQL 端，會傳給遠端腳本）
        # ==========================================
        "source_table": "mrt_VIRTUAL_TRANSACT_2",
        "date_column": "TABLE_DATE",
        # 也可以直接給整段 SQL，優先於 source_table + date_column
        # "source_sql": "SELECT * FROM dbo.T_TXN_PS WHERE TXN_DATE >= '{start_date}' AND TXN_DATE < '{end_date}'",

        # ==========================================
        # 2. 輸出檔案設定（都是 VM1 本機路徑）
        # ==========================================
        "output_folder": "/run/media/root/D/data/PostBS/Project_output/07_virtual_transact_2/",
        "template": "VIRTUAL_TRANSACT_2_EXPORT_{date}.csv",
        "freq": "daily",
        "delimiter": ",",
        "encoding": "utf-8-sig",

        # ==========================================
        # 3. 容錯機制
        # ==========================================
        "retries": 3,
        "retry_delay_sec": 60,

        # ==========================================
        # 4. 依賴的 dbt model（跑完這個 model 才觸發匯出）
        # ==========================================
        "depends_on_dbt_model": "GSS20250514_Virtual_Txn_Control_List_2",   # dbt model 的名字
        # 如果一個匯出要等多個 model 都跑完，可以給 list：
        # "depends_on_dbt_model": ["fct_txn_ps_reversal", "dim_customer"],

        # ==========================================
        # 5. 解密的欄位
        # ==========================================
        "decrypt_fields": ["帳號"],
    },
}
