Перед запускам нужно положить следующие файлы в папку `data`

```bash
.
├── data
│   ├── geocoding
│   │   ├── geocodes_checkpoint.parquet # (опционально, чтобы заново не парсить данные)
│   │   └── ya_api_keys.csv
│   └── raw
│       ├── Dataset_SCO_KVM_MONS_GRC_IZD_DMA_MTK_20250805.csv
│       ├── Dataset_SCO_KVM_MONS_GRC_IZD_DMA_MTK_20250805.xlsx
│       ├── Etagi_secondary_classified_dataset_20250805.csv
│       ├── Etagi_secondary_classified_dataset_20250805.xlsx
│       ├── Etagi_secondary_dataset_20250805.csv
│       ├── Etagi_secondary_dataset_20250805.xlsx
│       └── msk_united_geo_market_deals.parquet
```

В итоге получится такая структура
```bash
.
├── data
│   ├── geocoding
│   │   ├── geocodes_checkpoint.parquet # (если не было, то создастся)
│   │   └── ya_api_keys.csv
│   ├── processed
│   │   ├── housing_residential_processed.csv
│   │   └── housing_residential_processed.parquet
│   └── raw
│       ├── Dataset_SCO_KVM_MONS_GRC_IZD_DMA_MTK_20250805.csv
│       ├── Dataset_SCO_KVM_MONS_GRC_IZD_DMA_MTK_20250805.xlsx
│       ├── Etagi_secondary_classified_dataset_20250805.csv
│       ├── Etagi_secondary_classified_dataset_20250805.xlsx
│       ├── Etagi_secondary_dataset_20250805.csv
│       ├── Etagi_secondary_dataset_20250805.xlsx
│       └── msk_united_geo_market_deals.parquet
```