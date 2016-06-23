rm(list = ls())

library(foreign)

hucTable <- read.dbf("C:/KPONEIL/SHEDS/hucCats/results/manual_huc12s.dbf")

outTable <- hucTable[,c("FEATUREID", "new_huc12")]

names(outTable)[1] <- "featureid"

write.csv(outTable,
          file = "C:/KPONEIL/SHEDS/hucCats/results/manual_huc12s.csv", 
          row.names = F)