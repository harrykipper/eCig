int_trs <- function(x){
  xv <- as.vector(x)
  xint <- floor(xv)
  r <- xv - xint
  def <- round(sum(r)) # the deficit population
  # the weights be 'topped up' (+ 1 applied)
  topup <- sample(length(x), size = def, prob = r)
  xint[topup] <- xint[topup] + 1
  dim(xint) <- dim(x)
  dimnames(xint) <- dimnames(x)
  xint
}

int_expand_vector <- function(x){
  index <- 1:length(x)
  rep(index, round(x))
}

##############################################

library(future.apply)
library(dplyr)
plan(multiprocess, workers = 16)
library(ipfp)
reg<-unique(ind1674$region)
for(i in reg) {
  ## get all people and zones in the Local Authority
  cons_here<-cons1674[cons1674$region==i,]
  ind_here<-ind1674[ind1674$region==i,]
  
  ## Remove NAs from all dataframes --- POTENTIALLY DISRUPTIVE, but it will fail even with one NA 
  ind_here<-ind_here[complete.cases(ind_here),]
  #cons_here<-cons_here[complete.cases(cons_here),] ## Di questi non dovrebbero essercene
  
  for(s in unique(ind_here$ruc)) {
    ## Get all people and zones types: urban/rural
    cons_thiszone<-cons_here[cons_here$ruc==s,]
    ind_test<-ind_here[ind_here$ruc==s,]
    
    ## Get the relevant variables
    # cons_test<-cons_thiszone[c(2:80,84:93)]
    cons_test<-cons_thiszone[c(2:109)]
    zones<-data.frame(c(1:nrow(cons_thiszone)),cons_thiszone[,1])
    colnames(zones)<-c("zone","code")
    ## Remove NAs
    #ind_test<-ind_here[!is.na(ind_here$id),]
    
    ## IPFP
    cat_class<-model.matrix(~ ind_test$sec -1)[,c(1,2,5:10,4,3)]  ## 8 levels class
    #cat_class<-model.matrix(~ ind_test$seclong -1)[,c(1,19:37,2:18)] ## 35 levels class
    # cat_tenure<-model.matrix(~ ind_test$tenure -1)[,c(6,4,1,3,7,2,5)]  
    cat_ethni<-model.matrix(~ ind_test$ethni -1)[,c(17,8,6,13,18,15,16,12,7,14,3,5,10,1,4,11,2,9)]
    cat_sex <- model.matrix(~ ind_test$sex - 1)[, c(2, 1)]
    cat_age <- model.matrix(~ ind_test$ageclass - 1)
    
    colnames(cat_class)<-names(cons_test[72:108])
    colnames(cat_ethni)<-names(cons_test[54:71])
    colnames(cat_sex)<-names(cons_test[1:2])
    colnames(cat_age)<-names(cons_test[3:53])
    
    ind_cat<-cbind(cat_sex,cat_age,cat_ethni,cat_class)
    
    ## don't know which one. don't know the fucking difference
    weights <- array(NA, dim=c(nrow(ind_test),nrow(cons_test)))
    # weights<-matrix(data=1, nrow=nrow(ind_test), ncol=nrow(cons_test))
    
    # This is to test...
    # ind_agg <- matrix(colSums(ind_cat), nrow(cons_test), ncol(cons_test), byrow = T)
    
    cons_test_2<-future_apply(cons_test,2,as.numeric)
    weights<-future_apply(cons_test_2,MARGIN=1,FUN=function(x)ipfp(x,t(ind_cat),x0=rep(1,nrow(ind_test))))
    
    ints_df <- NULL
    ints <- unlist(future_apply(weights, 2, function(x) int_expand_vector(int_trs(x))))
    ints_df <- data.frame(id = ints, zone = rep(1:nrow(cons_test), round(colSums(weights))))
    ind_test$id <- 1:nrow(ind_test)
    ints_df <- inner_join(ints_df, ind_test)
    ints_df<-merge(ints_df,zones,by="zone")
    # write millions of agents into a csv
    write.csv(ints_df,file=paste(i,"-",s,".csv",sep=""),quote=FALSE,row.names = FALSE)
  }
}
