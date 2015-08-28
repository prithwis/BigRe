Sys.setenv(HADOOP_CMD="/usr/local/hadoop220/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop220/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar")

#Sys.getenv("HADOOP_CMD")
options(warn=-1)

library(rmr2)
library(rhdfs)
hdfs.init()

EstValue <- function (pdays,psale,N){
  days <-as.numeric(scan(text=pdays,,sep=" "))
  sale <-as.numeric(scan(text=psale,,sep=" "))
  regModel <- lm(sale ~ days)                	# <-- the all important R function lm()
  Est = predict(regModel,data.frame(days=N))
  OutRec = paste("Est[",N,Est,"] Dat:")
  for (ix in 1:length(days)){
    OutRec = paste(OutRec,days[ix],sale[ix])
  }
  OutRec = paste(OutRec,"\n")
  return(OutRec)
}

# Linear Regression Mapper Script - written by Prithwis Mukerjee
# Used in the Retail Sales Application

# Reads three pieces of data per line
# date, SKU, sale
# MR Mapper Key is sku
# MR Mapper Value is date$sale 

mapper2 = function(null,line) {
  ckey = line[[2]]
  cval = paste(line[[1]],line[[3]],sep = "$")
  keyval(ckey,cval)
}

# -- the Reducer
# 
# mapOut is the output data from the Mapper script
# read as table : first column = mapkey = SKU name
#               : second column = mapval 
# mapval consists of a string formatted as day$sale
# mapval needs to split into day, sale and then made into list to be passed to EstValue()

reducer2 = function(key,val.list) {
  firstVal = TRUE
  for(line in val.list) {
    DataVal <- unlist(strsplit(line, split="\\$"))
    if (firstVal) {
      days <- DataVal[[1]]
      sale <- DataVal[[2]]
      firstVal = FALSE
    } else {
      days <- paste(days,DataVal[[1]])
      sale <- paste(sale,DataVal[[2]])
    }
  }
  
  retVal <- EstValue(days,sale,9)
  keyval(key,retVal)
}

hdfs.ls("/user/hduser")
hdfs.rm("/user/hduser/Retail2-out")

# call MapReduce job
mapreduce(input="/user/hduser/Retail2-in",
          input.format=make.input.format("csv", sep="\t"),
          output="/user/hduser/Retail2-out",
          output.format="text",
          map=mapper2,
          reduce=reducer2
)

results <- from.dfs('/user/hduser/Retail2-out/part*',format="text")
results
