library(GA)
library(dplyr)

set.seed(112358)
A <- sample(1:100, 10, replace=T)

write.table(A,file="C:/Users/svaca/Documents/Code_cdrive/it_works.txt",quote=F,row.names=F,col.names=F,sep=";")



# 1) one-dimensional function
f <- function(x) abs(x)+cos(x)
curve(f, -20, 20)
fitness <- function(x) -f(x)
GA <- ga(type = "real-valued", fitness = fitness, lower = -20, upper = 20)
summary(GA)
plot(GA)
curve(f, -20, 20)
abline(v = GA@solution, lty = 3)

ga_test<- function(){
  f <- function(x) abs(x)+cos(x)
  fitness <- function(x) -f(x)
  GA <- ga(type = "real-valued", fitness = fitness, lower = -20, upper = 20, seed=1)
  return(GA@solution)
}

res = list()
for(i in 1:10){res[i] = ga_test()}

B <- res %>% data.frame() %>% t %>% data.frame() %>% unique()


write.table(B,file="C:/Users/svaca/Documents/Code_cdrive/it_works_ga.txt",quote=F,row.names=F,col.names=F,sep=";")
