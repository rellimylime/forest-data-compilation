library(here)
source(here("scripts/utils/metadata_utils.R"))

gdb_path <- here("01_ids/data/raw/CONUS_Region5_AllYears.gdb")

# 1. List layers
list_gdb_layers(gdb_path)

# 2. Summarize whole gdb
summarize_gdb(gdb_path)

# 3. Extract metadata from damage areas
metadata <- extract_gdb_metadata(gdb_path, "DAMAGE_AREAS_FLAT")

# 4. Load schema and validate
schema <- load_schema("ids")
validation <- validate_metadata(metadata, schema, "DAMAGE_AREAS_FLAT")
print(validation)

# 5. Generate data dictionary
dict <- generate_data_dictionary(metadata, schema, "DAMAGE_AREAS_FLAT")
write_data_dictionary(dict, here("01_ids/data_dictionary.csv"))