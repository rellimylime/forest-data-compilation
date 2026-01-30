# forest-data-compilation
Compiled and cleaned datasets for forest disturbance analysis: aerial detection surveys, climate data (TerraClimate, PRISM, WorldClim), and related environmental variables.

```
forest-data-compilation/
├── README.md
├── config.yaml
├── METADATA_MASTER.csv
├── .gitignore
├── renv.lock
├── .Rprofile
├── renv/
│
├── scripts/                           # Root-level universal scripts
│   ├── 00_setup.R
│   └── utils/
│       ├── load_config.R
│       ├── gee_utils.R               # Reusable GEE functions
│       └── metadata_utils.R                
│
├── local/                             # Gitignored, user-specific
│   └── user_config.yaml
│
├── 01_ids/
│   ├── README.txt
│   ├── schema.csv
│   ├── data_dictionary.csv
│   ├── cleaning_log.md
│   ├── WORKFLOW.md
├── docs/
│   ├── IDS2_FlatFiles_Readme.pdf
│   └── IDS2_TemplateFeatureClasses.xlsx
│   ├── scripts/
│   │   ├── 01_download_ids.R
│   │   ├── 02_inspect_ids.R
│   │   ├── 03_clean_ids.R
│   │   └── 04_prep_merge_ids.R
│   └── data/
│       ├── raw/
│       │   └── .gitkeep
│       └── processed/
│           └── .gitkeep
│
├── 02_terraclimate/
│   ├── README.txt
│   ├── data_dictionary.csv
│   ├── cleaning_log.md
│   ├── WORKFLOW.md
│   ├── scripts/
│   │   ├── 01_get_terraclimate.R      # Uses gee_utils.R
│   │   ├── 02_inspect_terraclimate.R
│   │   ├── 03_clean_terraclimate.R
│   │   └── 04_prep_merge_terraclimate.R
│   └── data/
│       ├── raw/
│       │   └── .gitkeep
│       └── processed/
│           └── .gitkeep
│
├── 03_prism/
│   ├── README.txt
│   ├── data_dictionary.csv
│   ├── cleaning_log.md
│   ├── WORKFLOW.md
│   ├── scripts/
│   │   └── .gitkeep
│   └── data/
│       ├── raw/
│       │   └── .gitkeep
│       └── processed/
│           └── .gitkeep
│
├── 04_worldclim/
│   ├── README.txt
│   ├── data_dictionary.csv
│   ├── cleaning_log.md
│   ├── WORKFLOW.md
│   ├── scripts/
│   │   └── .gitkeep
│   └── data/
│       ├── raw/
│       │   └── .gitkeep
│       └── processed/
│           └── .gitkeep
│
├── merged_data/
│   └── .gitkeep
│
└── templates/
    ├── WORKFLOW_TEMPLATE.md
    ├── data_dictionary_template.csv
    └── README_dataset_template.txt
```