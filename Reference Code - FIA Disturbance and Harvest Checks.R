############################################################
#Load Packages
library(tidyverse)
############################################################

############################################################
#Load Data
FIA.tree.dat<-data.table::fread("path/to/TREE table")
#Needed Columns in FIA.tree.dat: tree_cols <- c("PLT_CN", "STATECD", "UNITCD", "COUNTYCD", "STATUSCD", "INVYR", "PLOT", "SUBP", "TREE", "SPCD", "TPA_UNADJ", "DIA", "AGENTCD")

FIA.cond<-data.table::fread("path/to/ENTIRE_COND.csv")

############################################################
############################################################
############################################################
#Tree Data Checks
############################################################
############################################################
############################################################

#########################
#Checks Individual Trees for Incidental Tree Harvesting (Not Counted in "Logging", Could be Misinterpreted as Natural Mortality if not Flagged)
#NOTE: After all checks may also want to trim to "STATUSCD == 1" (Live Trees) - but depends on analysis

incidental.harvests<-FIA.tree.dat%>%
  mutate(site = paste0(STATECD, "_", UNITCD, "_", COUNTYCD, "_", PLOT, "_"),                                 
         std_id = paste0("forest_", "FIA_", site))%>%
  rename(year = INVYR)%>%
  mutate(tree_id = paste0(STATECD, "_", UNITCD, "_", COUNTYCD, "_", PLOT, "_", SUBP, "_", TREE))%>% #Creates Unique Tree Identifier
  group_by(tree_id)%>%
         mutate(harvest_flag = if_else(any(AGENTCD %in% c(80, 81, 82, 83, 84, 85, 86, 87, 88, 89)), "harvested", NA_character_))%>% #Incidental Tree Harvesting Flag
  ungroup()

############################################################
############################################################
############################################################
#Condition Table Checks
############################################################
############################################################
############################################################

#########################
#Checks for Condition "5" - Non-Forest Land with Trees 

condition_5_check<-FIA.cond%>%
  mutate(site = paste0(STATECD, "_", UNITCD, "_", COUNTYCD, "_", PLOT, "_"),                                 
         std_id = paste0("forest_", "FIA_", site))%>%
  select(std_id, INVYR, CONDPROP_UNADJ, COND_STATUS_CD)%>%
  distinct()%>%
  rename(year = INVYR)%>%
  group_by(std_id, year)%>%
  summarise(
    condition.count = n(),
    condition.check = mean(CONDPROP_UNADJ, na.rm = TRUE),
    all_conds = paste(unique(COND_STATUS_CD), collapse = "_"),
    .groups = "drop"
  )%>%
  filter(grepl("5", all_conds))%>%
  mutate(has_condition_5 = "x")%>%
  select(std_id, year, has_condition_5)

#########################
#Checks for TRT (Treatment) Values

# TRTCD lookup
trt_lookup <- data.frame(
  code = c(0, 10, 20, 30, 40, 50),
  description = c("No observable treatment",
                  "Cutting",
                  "Site preparation",
                  "Artificial regeneration",
                  "Natural regeneration",
                  "Other silvicultural treatment")
)

trt_check<-FIA.cond%>%
  mutate(site = paste0(STATECD, "_", UNITCD, "_", COUNTYCD, "_", PLOT, "_"),                                 
         std_id = paste0("forest_", "FIA_", site))%>%
  select(std_id, CONDID, INVYR, TRTCD1, TRTYR1, TRTCD2, TRTYR2, TRTCD3, TRTYR3, PLT_CN)%>%
  distinct()%>%
  left_join(trt_lookup, by = c("TRTCD1" = "code")) %>%
  rename(TRT1_DESC = description) %>%
  left_join(trt_lookup, by = c("TRTCD2" = "code")) %>%
  rename(TRT2_DESC = description) %>%
  left_join(trt_lookup, by = c("TRTCD3" = "code")) %>%
  rename(TRT3_DESC = description)

#########################
#Checks for Disturbances

FIA.disturbances<-FIA.cond%>%
  mutate(site = paste0(STATECD, "_", UNITCD, "_", COUNTYCD, "_", PLOT, "_"),                                 
         std_id = paste0("forest_", "FIA_", site))%>%
  filter(!is.na(DSTRBCD1))%>%
  mutate(disturbance_1 = case_when(DSTRBCD1 == 0 ~ "None",
                                   DSTRBCD1 == 10 ~ "Insect - Overall",
                                   DSTRBCD1 == 11 ~ "Insect - Understory",
                                   DSTRBCD1 == 12 ~ "Insect - Tree",
                                   DSTRBCD1 == 20 ~ "Disease - Overall",
                                   DSTRBCD1 == 21 ~ "Disease - Understory",
                                   DSTRBCD1 == 22 ~ "Disease - Tree",
                                   DSTRBCD1 == 30 ~ "Fire - Overall",
                                   DSTRBCD1 == 31 ~ "Fire - Ground",
                                   DSTRBCD1 == 32 ~ "Fire - Crown",
                                   DSTRBCD1 == 40 ~ "Animal Damage",
                                   DSTRBCD1 == 41 ~ "Beaver",
                                   DSTRBCD1 == 42 ~ "Porcupine",
                                   DSTRBCD1 == 43 ~ "Deer/Ungulate",
                                   DSTRBCD1 == 44 ~ "Bear",
                                   DSTRBCD1 == 45 ~ "Rabbit",
                                   DSTRBCD1 == 46 ~ "Domestic Animal/Livestock",
                                   DSTRBCD1 == 50 ~ "Weather",
                                   DSTRBCD1 == 51 ~ "Ice",
                                   DSTRBCD1 == 52 ~ "Wind",
                                   DSTRBCD1 == 53 ~ "Flooding",
                                   DSTRBCD1 == 54 ~ "Drought",
                                   DSTRBCD1 == 60 ~ "Vegetation (supp/comp/vines)",
                                   DSTRBCD1 == 70 ~ "Unknown",
                                   DSTRBCD1 == 80 ~ "Human-induced",
                                   DSTRBCD1 == 90 ~ "Geologic Disturbance",
                                   DSTRBCD1 == 91 ~ "Landslide",
                                   DSTRBCD1 == 92 ~ "Avalanche",
                                   DSTRBCD1 == 93 ~ "Volcanic Blast Zone",
                                   DSTRBCD1 == 94 ~ "Other Geologic Event",
                                   DSTRBCD1 == 95 ~ "Earth Movement / Avalanche",
                                   TRUE ~ as.character(DSTRBCD1)),
         disturbance_2 = case_when(DSTRBCD2 == 0 ~ "None",
                                   DSTRBCD2 == 10 ~ "Insect - Overall",
                                   DSTRBCD2 == 11 ~ "Insect - Understory",
                                   DSTRBCD2 == 12 ~ "Insect - Tree",
                                   DSTRBCD2 == 20 ~ "Disease - Overall",
                                   DSTRBCD2 == 21 ~ "Disease - Understory",
                                   DSTRBCD2 == 22 ~ "Disease - Tree",
                                   DSTRBCD2 == 30 ~ "Fire - Overall",
                                   DSTRBCD2 == 31 ~ "Fire - Ground",
                                   DSTRBCD2 == 32 ~ "Fire - Crown",
                                   DSTRBCD2 == 40 ~ "Animal Damage",
                                   DSTRBCD2 == 41 ~ "Beaver",
                                   DSTRBCD2 == 42 ~ "Porcupine",
                                   DSTRBCD2 == 43 ~ "Deer/Ungulate",
                                   DSTRBCD2 == 44 ~ "Bear",
                                   DSTRBCD2 == 45 ~ "Rabbit",
                                   DSTRBCD2 == 46 ~ "Domestic Animal/Livestock",
                                   DSTRBCD2 == 50 ~ "Weather",
                                   DSTRBCD2 == 51 ~ "Ice",
                                   DSTRBCD2 == 52 ~ "Wind",
                                   DSTRBCD2 == 53 ~ "Flooding",
                                   DSTRBCD2 == 54 ~ "Drought",
                                   DSTRBCD2 == 60 ~ "Vegetation (supp/comp/vines)",
                                   DSTRBCD2 == 70 ~ "Unknown",
                                   DSTRBCD2 == 80 ~ "Human-induced",
                                   DSTRBCD2 == 90 ~ "Geologic Disturbance",
                                   DSTRBCD2 == 91 ~ "Landslide",
                                   DSTRBCD2 == 92 ~ "Avalanche",
                                   DSTRBCD2 == 93 ~ "Volcanic Blast Zone",
                                   DSTRBCD2 == 94 ~ "Other Geologic Event",
                                   DSTRBCD2 == 95 ~ "Earth Movement / Avalanche",
                                   TRUE ~ as.character(DSTRBCD2)),
         disturbance_3 = case_when(DSTRBCD3 == 0 ~ "None",
                                   DSTRBCD3 == 10 ~ "Insect - Overall",
                                   DSTRBCD3 == 11 ~ "Insect - Understory",
                                   DSTRBCD3 == 12 ~ "Insect - Tree",
                                   DSTRBCD3 == 20 ~ "Disease - Overall",
                                   DSTRBCD3 == 21 ~ "Disease - Understory",
                                   DSTRBCD3 == 22 ~ "Disease - Tree",
                                   DSTRBCD3 == 30 ~ "Fire - Overall",
                                   DSTRBCD3 == 31 ~ "Fire - Ground",
                                   DSTRBCD3 == 32 ~ "Fire - Crown",
                                   DSTRBCD3 == 40 ~ "Animal Damage",
                                   DSTRBCD3 == 41 ~ "Beaver",
                                   DSTRBCD3 == 42 ~ "Porcupine",
                                   DSTRBCD3 == 43 ~ "Deer/Ungulate",
                                   DSTRBCD3 == 44 ~ "Bear",
                                   DSTRBCD3 == 45 ~ "Rabbit",
                                   DSTRBCD3 == 46 ~ "Domestic Animal/Livestock",
                                   DSTRBCD3 == 50 ~ "Weather",
                                   DSTRBCD3 == 51 ~ "Ice",
                                   DSTRBCD3 == 52 ~ "Wind",
                                   DSTRBCD3 == 53 ~ "Flooding",
                                   DSTRBCD3 == 54 ~ "Drought",
                                   DSTRBCD3 == 60 ~ "Vegetation (supp/comp/vines)",
                                   DSTRBCD3 == 70 ~ "Unknown",
                                   DSTRBCD3 == 80 ~ "Human-induced",
                                   DSTRBCD3 == 90 ~ "Geologic Disturbance",
                                   DSTRBCD3 == 91 ~ "Landslide",
                                   DSTRBCD3 == 92 ~ "Avalanche",
                                   DSTRBCD3 == 93 ~ "Volcanic Blast Zone",
                                   DSTRBCD3 == 94 ~ "Other Geologic Event",
                                   DSTRBCD3 == 95 ~ "Earth Movement / Avalanche",
                                   TRUE ~ as.character(DSTRBCD3)),
         d_year1 = DSTRBYR1,
         d_year2 = DSTRBYR2,
         d_year3 = DSTRBYR3,
         year = INVYR)%>%
  select(std_id, year, disturbance_1, d_year1, disturbance_2, d_year2, disturbance_3, d_year3)%>%
  filter(disturbance_1 != "None")%>%
  distinct()
