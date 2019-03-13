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

library(dplyr)
library(future.apply)
plan(multiprocess, workers = 16)
library(ipfp)
reg<-unique(ind1664[ind1664$region!="Scotland" & ind1664$region!="Northern Ireland",]$region)
for(i in toupper(reg)) {
  ## get all people and zones in the Local Authority
  cons_here<-cons1674[toupper(cons1674$region)==i,]
  ind_here<-ind1674AllSEC[toupper(ind1674AllSEC$region)==i,]
  
  # USE THIS if we want to generate the population based only on those with the 
  # smoking/non-smoking attribute.
  # ============================================================================
  # ind_here<-merge(ind_here,res[!is.na(res$smoker) & res$smoker>0,c(1,31)],by="pidp")
  # ============================================================================
  
  ## Remove NAs from all dataframes --- POTENTIALLY DISRUPTIVE, but it will fail even with one NA 
  ind_here<-ind_here[complete.cases(ind_here),]
  ## Di questi non dovrebbero essercene. Se ci sono MORTE
  #cons_here<-cons_here[complete.cases(cons_here),] 
  
  for(s in unique(ind_here$ruc)) {
    ## Get all people and zones types: urban/rural
    cons_thiszone<-cons_here[cons_here$ruc==s,]
    ind_test<-ind_here[ind_here$ruc==s,]
    cat("Now processing", i, "(",s,")")
    ## Get the relevant variables
    # cons_test<-cons_thiszone[c(2:80,84:93)]
    # cons_test<-cons_thiszone[c(2:79)]
    cons_test<-cons_thiszone[c(2:3,112:123,55:109)]
    zones<-data.frame(c(1:nrow(cons_thiszone)),cons_thiszone[,1])
    colnames(zones)<-c("zone","code")
    ## Remove NAs
    #ind_test<-ind_here[!is.na(ind_here$id),]
    
    ## IPFP
    # cat_class<-model.matrix(~ ind_test$sec -1)[,c(1,2,5:10,4,3)]  ## 8 levels class
    #cat_class<-model.matrix(~ ind_test$secshort -1)[,c(3,2,9,8,7,6,5,4,1)]  ## 7 levels class
    ## WARNING - Class column names in ind1664 are REVERSED, but not the content (I hope)
    
    cat_class<-model.matrix(~ ind_test$seclong -1)[,c(1,19:37,2:18)] ## 35 levels class
    # cat_class<-model.matrix(~ ind_test$secshort -1)[,c(1,4:9,2,3)] ## 8 levels class
    # cat_tenure<-model.matrix(~ ind_test$tenure -1)[,c(6,4,1,3,7,2,5)]  
    cat_ethni<-model.matrix(~ ind_test$ethni -1)[,c(17,8,6,13,18,15,16,12,7,14,3,5,10,1,4,11,2,9)]
    cat_sex <- model.matrix(~ ind_test$sex - 1)[, c(2, 1)]
    cat_age <- model.matrix(~ ind_test$ageclass - 1)
    
    colnames(cat_class)<-names(cons_test[33:69])
    colnames(cat_ethni)<-names(cons_test[15:32])
    colnames(cat_sex)<-names(cons_test[1:2])
    colnames(cat_age)<-names(cons_test[3:14])
    ind_cat<-cbind(cat_sex,cat_age,cat_ethni,cat_class)
    
    ## don't know which one. don't know the fucking difference
    # weights <- array(NA, dim=c(nrow(ind_test),nrow(cons_test)))
    weights<-matrix(data=1, nrow=nrow(ind_test), ncol=nrow(cons_test))
    
    # This is to test...
    #ind_agg <- matrix(colSums(ind_cat), nrow(cons_test), ncol(cons_test), byrow = T)
    #ind_agg <- t(future_apply(weights, 2, function(x) colSums(x * ind_cat)))
    #colnames(ind_agg) <- colnames(cons_test)
    
    suffix<-""
    cons_test_2<-future_apply(cons_test,2,as.numeric)
    weights<-future_apply(cons_test_2,MARGIN=1,FUN=function(x)ipfp(x,t(ind_cat),x0=rep(1,nrow(ind_test))))
    nulls<-length(weights[is.na(weights)])
    ints_df <- NULL
    tryCatch({
      if(nulls > 0){
        if(nulls > length(weights[!is.na(weights)])){
          stop()
        }
        suffix<-"_problematic"
        weights[is.na(weights)]<-0.00000001
      }
      ints <- unlist(future_apply(weights, 2, function(x) int_expand_vector(int_trs(x))))
      ints_df <- data.frame(id = ints, zone = rep(1:nrow(cons_test), round(colSums(weights))))
      ind_test$id <- 1:nrow(ind_test)
      ints_df <- inner_join(ints_df, ind_test)
      ints_df<-merge(ints_df,zones,by="zone")
      assign(paste0(gsub(" ", "", i, fixed = TRUE),"_1674",suffix),ints_df)
      # write millions of agents into a csv
      write.csv(ints_df,file=paste(gsub(" ", "", i, fixed = TRUE),"-",s,suffix,".csv",sep=""),quote=FALSE,row.names = FALSE)
    }, error=function(e){cat("--ERROR :",i, "(", s, ")", conditionMessage(e), "\n")})
  }
}

# give them smoking values
dflist<-list(df1=NORTHWEST,df2=SOUTHEAST,df3=SOUTHWEST_problematic,df4=LONDON_problematic,
             df5=EASTMIDLANDS,df6=EASTOFENGLAND,df7=WALES_problematic,df8=WESTMIDLANDS_problematic,
             df9=YORKSHIREANDTHEHUMBER_problematic)

dflist<-list(df1=NORTHWEST_problematic,df2=SOUTHEAST,df4=LONDON_problematic,
             df5=EASTMIDLANDS_problematic,df6=EASTOFENGLAND,df9=YORKSHIREANDTHEHUMBER_problematic)

for(f in 1:length(dflist)){
  dflist[[f]]<-merge(dflist[[f]],res[c(1,31)],by="pidp", all.x = TRUE)
}

library(reshape)

for(q in 1:length(dflist)){
  smokerz<-dflist[[q]][!is.na(dflist[[q]]$smoker)&(dflist[[q]]$smoker>0),c(12:13)]
  fava<-melt(smokerz, id=c("code")) 
  lota<-cast(fava, code~value, length)
  colnames(lota)<-c("zone","smokers","nonsmokers")
  lota$ratio<-lota$smokers/(lota$nonsmokers+lota$smokers)
  write.csv(lota,paste0("~/ownCloud/sphsu/df-",q,"_smokers.csv"),row.names = FALSE)
}

smokerz<-NORTHWEST_problematic[!is.na(NORTHWEST_problematic$smoker) & NORTHWEST_problematic$smoker>0,c(12:13)]

future_lapply(dflist, function(q){
  smokerz<-q[q$c_evrsmo>0,c(12:13)]
  fava<-melt(smokerz, id=c("code")) 
  lota<-cast(fava, code~value, length)
  colnames(lota)<-c("zone","smokers","nonsmokers")
  lota$ratio<-lota$smokers/(lota$nonsmokers+lota$smokers)
  write.csv(lota,paste0("~/ownCloud/sphsu/",q,"-smokers.csv"),row.names = FALSE)
})