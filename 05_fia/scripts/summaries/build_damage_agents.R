# ------------------------------------------------------------------------------
# build_damage_agents
# ------------------------------------------------------------------------------

build_damage_agents <- function(out_dir, proc_fia) {
  # Step 6: plot_damage_agents
  # Collect per-state damage_agents parquets and join agent code labels
  # ------------------------------------------------------------------------------
  
  cat("Step 6: plot_damage_agents\n")
  out_damage_ag <- file.path(out_dir, "plot_damage_agents.parquet")

  rb <- fia_should_rebuild(
    out_damage_ag,
    input_paths = here(proc_fia$damage_agents$output_dir),
    required_cols = c("PLT_CN", "INVYR", "CONDID", "SPCD", "DAMAGE_AGENT_CD"),
    label = "plot_damage_agents"
  )
  if (!rb$rebuild) {
    cat(glue("  Up to date ({rb$reason}, {file_size(out_damage_ag)}) - skipping\n\n"))
  } else {
    if (file_exists(out_damage_ag)) cat(glue("  Rebuilding ({rb$reason})\n"))
    da_ds <- tryCatch(
      open_dataset(here(proc_fia$damage_agents$output_dir), partitioning = "state"),
      error = function(e) NULL
    )
    if (is.null(da_ds)) {
      cat("  No damage_agents parquets found. Run 03_extract_trees.R first.\n\n")
    } else {
      # Inline damage agent lookup â€” category codes + ~30 high-profile species
      # Source: FIADB v9.4 Appendix H (PTIPS/FHAAST codes)
      ref_damage_agent <- data.table(
        DAMAGE_AGENT_CD = c(
          # Category-level codes
          10000L, 11000L, 12000L, 13000L, 14000L, 15000L, 16000L, 17000L, 18000L,
          19000L, 20000L, 21000L, 22000L, 22500L, 23000L, 24000L, 25000L, 26000L, 27000L,
          30000L, 30001L, 30002L, 30003L, 30004L,
          41000L, 42000L, 50000L, 60000L, 70000L, 71000L, 80000L, 90000L, 99000L,
          # Named bark beetle species (11xxx)
          11003L, 11006L, 11007L, 11009L, 11010L, 11019L, 11023L, 11029L, 11045L,
          11800L, 11900L, 11999L,
          # Named defoliators (12xxx)
          12038L, 12039L, 12040L, 12041L, 12083L, 12089L, 12096L, 12197L,
          12800L, 12900L,
          # Sucking insects (14xxx)
          14003L, 14004L, 14016L, 14800L, 14900L,
          # Boring insects (15xxx)
          15082L, 15087L, 15090L, 15800L, 15900L,
          # Root/butt diseases (21xxx)
          21001L, 21010L, 21014L, 21017L, 21019L, 21028L, 21800L, 21900L,
          # Cankers & rusts (22xxx, 26xxx)
          22023L, 22042L, 22086L, 22300L,
          26001L, 26009L, 26800L,
          # Other disease (24xxx, 25xxx)
          24021L, 24022L, 24031L, 24800L,
          25040L, 25043L, 25800L
        ),
        agent_label = c(
          "General insects", "Bark beetles", "Defoliators", "Chewing insects",
          "Sucking insects (adelgids/scales/aphids)", "Boring insects",
          "Seed/cone insects", "Gallmakers", "Insect predators",
          "General diseases", "Biotic damage", "Root/butt diseases",
          "Cankers (non-rust)", "Stem decay", "Parasitic/epiphytic plants",
          "Decline complexes/dieback/wilts", "Foliage diseases",
          "Stem rusts", "Broom rusts",
          "Fire", "Wildfire", "Human-caused fire", "Crown fire", "Ground fire",
          "Wild animals", "Domestic animals", "Abiotic damage",
          "Competition", "Human activities", "Harvest",
          "Multi-damage insect/disease complex", "Other damages", "Unknown",
          # Bark beetles
          "Southern pine beetle", "Mountain pine beetle", "Douglas-fir beetle",
          "Spruce beetle", "Eastern larch beetle", "Pinon ips",
          "Southern pine engraver", "Pine engraver", "Small European elm bark beetle",
          "Other bark beetle (known)", "Unknown bark beetle", "Western bark beetle complex",
          # Defoliators
          "Spruce budworm", "Western pine budworm", "Western spruce budworm",
          "Jack pine budworm", "Hemlock looper", "Gypsy moth",
          "Forest tent caterpillar", "Winter moth",
          "Other defoliator (known)", "Unknown defoliator",
          # Sucking insects
          "Balsam woolly adelgid", "Hemlock woolly adelgid", "Beech scale",
          "Other sucking insect (known)", "Unknown sucking insect",
          # Boring insects
          "Asian longhorned beetle", "Emerald ash borer", "Sirex woodwasp",
          "Other boring insect (known)", "Unknown boring insect",
          # Root/butt diseases
          "Armillaria root disease", "Heterobasidion root disease",
          "Black stain root disease", "Laminated root rot",
          "Phytophthora root rot / littleleaf disease", "Sudden oak death",
          "Other root/butt disease (known)", "Unknown root/butt disease",
          # Cankers & rusts
          "Chestnut blight", "Beech bark disease",
          "Thousand cankers disease", "Other canker (known)",
          "White pine blister rust", "Fusiform rust", "Other stem rust (known)",
          # Other disease
          "Oak wilt", "Dutch elm disease", "Laurel wilt", "Other decline/wilt (known)",
          "Dothistroma needle blight", "Swiss needle cast", "Other foliage disease (known)"
        ),
        agent_category = c(
          "insects", "insects", "insects", "insects", "insects", "insects",
          "insects", "insects", "insects",
          "disease", "disease", "disease", "disease", "disease", "disease",
          "disease", "disease", "disease", "disease",
          "fire", "fire", "fire", "fire", "fire",
          "animal", "animal", "abiotic", "competition", "human", "human",
          "complex", "other", "unknown",
          # Bark beetles
          rep("bark beetles", 12),
          # Defoliators
          rep("defoliators", 10),
          # Sucking insects
          rep("sucking insects", 5),
          # Boring insects
          rep("boring insects", 5),
          # Root/butt diseases
          rep("root/butt disease", 8),
          # Cankers & rusts
          rep("canker/rust", 7),
          # Other disease
          rep("foliage/wilt disease", 7)
        )
      )
      setkey(ref_damage_agent, DAMAGE_AGENT_CD)
  
      all_da <- da_ds |> collect() |> as.data.table()
      setkey(all_da, DAMAGE_AGENT_CD)
      # Left join: keep all records, label known codes, leave others as NA
      all_da <- ref_damage_agent[all_da, on = "DAMAGE_AGENT_CD"]
  
      write_parquet_atomic(as_tibble(all_da), out_damage_ag, compression = "snappy")
      cat(glue("  plot_damage_agents: {format(nrow(all_da), big.mark=',')} rows -> ",
               "{file_size(out_damage_ag)}\n\n"))
      rm(all_da); gc(verbose = FALSE)
    }
  }
  
  # ------------------------------------------------------------------------------

  out_damage_ag
}

