setwd("/home/hduser/BigRegression")
getwd()

bd = read.csv("web_traffic.csv",header = FALSE)

hour = bd$V1
hits = bd$V2
plot(hour,hits)

m1 =lm(hits ~poly(hour,1,raw = TRUE))
summary(m1)
m2 = lm(hits~poly(hour,2),raw=TRUE )
summary(m2)
m3 = lm(hits~poly(hour,3),raw = TRUE )
summary(m3)

lines(hour,predict(m1,data.frame(x=hour)), col='yellow')
lines(hour,predict(m2,data.frame(x=hour)), col='red')
lines(hour,predict(m3,data.frame(x=hour)), col='green')

predict(m1,data.frame(hour=300))
predict(m2,data.frame(hour=300))
predict(m3,data.frame(hour=300))




