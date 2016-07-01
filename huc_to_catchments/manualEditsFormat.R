# ===========
# Description
# ===========

# This script converts the ArcGIS output table (dbf format) to CSV format for 
#   upload to the SHEDS database


# ==============
# Load Libraries
# ==============
rm(list = ls())

library(foreign)


# ==========
# Processing
# ==========
hucTable <- read.dbf("C:/KPONEIL/SHEDS/hucCats/results/manual_huc12s.dbf")

outTable <- hucTable[,c("FEATUREID", "new_huc12")]

names(outTable) <- c("featureid", "huc12")

write.csv(outTable,
          file = "C:/KPONEIL/SHEDS/hucCats/results/manual_huc12s.csv", 
          row.names = F)