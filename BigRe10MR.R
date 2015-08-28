Sys.setenv(HADOOP_CMD="/usr/local/hadoop220/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop220/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar")

Sys.getenv("HADOOP_CMD")
options(warn=-1)
library(rmr2)
library(rhdfs)
hdfs.init()

mapper5 = function(null,line) {
  ckey = strtoi(line[[1]])%%20+1
  cval = paste(line[[1]],line[[2]],sep = "$")
  keyval(ckey,cval)
}

EstValue5 <- function (px,py,N){
  x <-as.numeric(scan(text=px,,sep=" "))
  y <-as.numeric(scan(text=py,,sep=" "))
  regModel <- lm(y ~ poly(x,2,raw=TRUE))                  # <-- the all important R function lm()
  RetVal = round(predict(regModel,data.frame(x=N)))
  return(RetVal)
}

reducer5 = function(key,val.list) {
  firstVal = TRUE
  for(line in val.list) {
    DataVal <- unlist(strsplit(line, split="\\$"))
    if (firstVal) {
      hour = DataVal[[1]]
      hits = DataVal[[2]]
      firstVal = FALSE
    } else{
      hour = paste(hour,DataVal[[1]])
      hits = paste(hits,DataVal[[2]])
    }
  }
  keyval(key,EstValue5(hour,hits,300))
}

hdfs.ls("/user/hduser")
hdfs.rm("/user/hduser/BigRe/out5")

# call MapReduce job
mapreduce(input="/user/hduser/BigRe/in",
          input.format=make.input.format("csv",sep=","),
          output="/user/hduser/BigRe/out5",
          output.format="text",
          map=mapper5,
          reduce=reducer5
)

results5 <- from.dfs('/user/hduser/BigRe/out5/part*',format="text")
results5$val
EstValues = strtoi(sub(".*\\t","",results5$val))
summary(EstValues)

CI = function (x,level) {
err = qnorm(1-(100-level)/200)*sd(x)/sqrt(length(x))
cat(level,"% CI[",mean(x)-err,"--",mean(x)+err,"] with Mean = ",mean(x)) 
}

CI(EstValues,95)
