#! /usr/bin/env Rscript

# Linear Regression Reducer Script - written by Prithwis Mukerjee
# Used in the Retail Sales Application

# EstValue is function called to implement the linear regression function of R
#
# Parameters are as follows
# pSKU - string - name of SKU
# pdays - list of days for which data is available
# psale - list of values of sale data
# Nth - integer - N-th day for which the estimate will be made
#
# Other variables are as follows

# Est - estimated sale for a particular day

EstValue <- function (pdays,psale,N){
  days <-as.numeric(scan(text=pdays,,sep=" "))
  sale <-as.numeric(scan(text=psale,,sep=" "))
  regModel <- lm(sale ~ days)              		# <-- the all important R function lm()
  Est = predict(regModel,data.frame(days=N))
  OutRec = paste("Est[",N,Est,"] Dat:")
  for (ix in 1:length(days)){
  OutRec = paste(OutRec,days[ix],sale[ix])
  }
  OutRec = paste(OutRec,"\n")
  return(OutRec)
  
}

# -- the Reducer
# 
# mapOut is the output data from the Mapper script
# read as table : first column = mapkey = SKU name
#               : second column = mapval 
# mapval consists of a string formatted as day$sale
# mapval needs to split into date, sale and then made into list to be passed to EstValue()

mapOut <- read.table("stdin",col.names=c("mapkey","mapval"))
CurrSKU <- as.character(mapOut[1,]$mapkey)
CurrVal <- ""
FIRSTROW = TRUE
for(i in 1:nrow(mapOut)){
  SKU <- as.character(mapOut[i,]$mapkey)
  Val <- as.character(mapOut[i,]$mapval)
  DataVal <- unlist(strsplit(Val,"\\$"))
  if (identical(SKU,CurrSKU)){
    CurrVal = paste(CurrVal, Val)
    if (FIRSTROW)  {
      days <- DataVal[1]
      sale <- DataVal[2]
      FIRSTROW = FALSE
    } else {
    days = paste(days,DataVal[1])
    sale = paste(sale,DataVal[2])
  }
  }
  else {
    cat(CurrSKU,EstValue(days,sale,9))
    CurrSKU <- SKU
    CurrVal <- Val
    days <- DataVal[1]
    sale <- DataVal[2]
    
  }
}
cat(CurrSKU,EstValue(days,sale,9))
