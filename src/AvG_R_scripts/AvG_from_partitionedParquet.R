print("_________________")
.libPaths(c( "C:/Users/svaca/Documents/R/win-library/3.5" , "C:/Program Files/R/R-3.5.2/library"))

library(dplyr)
library(tidyr)
library(stringr)
library(data.table)
library(GA)
library(AvantGardeDIA)
packageVersion("AvantGardeDIA")
#source("C:/Users/svaca/Documents/Code_saved/GeneticAlgorithm/20190118_AvantGardeForPublication/r_package/AvantGardeDIA/R/AvantGardeDIA.R")

args <- commandArgs(TRUE)
analyte_data <- as.character(args[1])
params_file <- as.character(args[2])
analyte_hash_id <- as.character(args[3])
output_dir <- as.character(args[4])

print(args)
# args <- commandArgs(TRUE)
# analyte_data <- "C:/Users/svaca/Documents/Code_saved/GeneticAlgorithm/Luigi_AvG_workflow/2019sp-final-project-SebVaca/data/csv_ds/data_analyte_4ada7661.csv"
# params_file <-  "C:/Users/svaca/Documents/Example_AvG/luigi_test/AvantGardeDIA/AvG_Params.R"
# analyte_hash_id <- "4ada7661"
# output_dir <-  "C:/Users/svaca/Documents/Code_saved/GeneticAlgorithm/Luigi_AvG_workflow/2019sp-final-project-SebVaca/data/avg_results/"

#############################################


Filter_Na_Shared_Or_LowMassTransitions_modif<-function(D1){
  # Remove NA replictaes (Non integrated peaks) and remove transitions y1-3 and b1-3
  D1<-as.data.frame(D1) %>% filter(!is.na(as.numeric(D1$MinStartTime))) %>%
    mutate(Transition_Filter=paste0(FragmentIon," "))%>%
    mutate(Transition_Filter=substr(Transition_Filter,start=1,stop = str_locate(Transition_Filter,pattern = " ")-1)) %>%
    filter(!grepl(paste(c("precursor",paste0("^y", 0:RemoveTransitionBelowOrdinal_Filter,"$"),paste0("^b",0:RemoveTransitionBelowOrdinal_Filter,"$")), collapse="|"), Transition_Filter)) %>%
    select(-Transition_Filter)
  
  ## Remove transition that are not in light AND in the heavy version
  A0<-D1 %>% select(ID_Analyte,ID_FragmentIon_charge,IsotopeLabelType) %>% 
    distinct() %>% group_by(ID_Analyte) %>% summarise(TotalNumLabels=length(unique(IsotopeLabelType)))
  
  A1<-D1 %>% select(ID_Analyte,ID_FragmentIon_charge,IsotopeLabelType) %>% 
    distinct() %>%
    group_by(ID_Analyte,ID_FragmentIon_charge) %>% 
    summarise(NumLabels=length(unique(IsotopeLabelType))) %>%
    left_join(A0, by = "ID_Analyte") %>%
    mutate(diff=TotalNumLabels-NumLabels) %>%
    filter(diff!=0) %>% select(ID_Analyte,ID_FragmentIon_charge) %>%
    ungroup %>%
    mutate(ID_Analyte=as.integer(ID_Analyte),ID_FragmentIon_charge=as.integer(ID_FragmentIon_charge),Remove="DoNotKeep")
  
  if (dim(A1)[1]>=1) {
    D1<-D1 %>% left_join(A1, by = c("ID_Analyte", "ID_FragmentIon_charge")) %>% filter(is.na(Remove)) %>% select(-Remove)
  }
  
  if(RemoveSharedTransitionsBetweenLightAndHeavy_Filter==TRUE){
    ## Remove shared transitions between light and heavy (b ions if the labeling is in C-ter)
    RemoveSharedTransitionsLightAndHeavy<-D1 %>% select(ProteinName,PeptideModifiedSequence,PrecursorCharge,ProductMz,ProductCharge,FragmentIon,IsotopeLabelType,IsDecoy) %>%
      distinct() %>%
      group_by(ProteinName,PeptideModifiedSequence,PrecursorCharge,ProductMz,IsDecoy) %>%
      tally %>%
      filter(n>1) %>% ungroup %>%
      select(ProteinName,PeptideModifiedSequence,PrecursorCharge,ProductMz,IsDecoy) %>%
      mutate(Remove="Remove")
    
    ## Filter less than MinimalInitialNumberOfTransitions_Filter transitions
    FilterLessThanNTrans<-D1 %>%
      select(ProteinName,PeptideModifiedSequence,PrecursorCharge,IsotopeLabelType,FragmentIon,ProductCharge,IsDecoy) %>%
      distinct()  %>%
      group_by(ProteinName,PeptideModifiedSequence,PrecursorCharge,IsotopeLabelType,IsDecoy) %>%
      tally() %>%
      mutate(Keep=ifelse(n>=MinimalInitialNumberOfTransitions_Filter,"keep","DoNOTKeep")) %>%
      select(ProteinName,PeptideModifiedSequence,PrecursorCharge,IsotopeLabelType,IsDecoy,Keep)
    
    
    D1<- D1 %>% left_join(RemoveSharedTransitionsLightAndHeavy,by=c("ProteinName","PeptideModifiedSequence","PrecursorCharge","ProductMz","IsDecoy")) %>%
      filter(is.na(Remove)) %>%
      select(-Remove) %>%
      left_join(data.frame(FilterLessThanNTrans),by = c("ProteinName","PeptideModifiedSequence", "PrecursorCharge", "IsotopeLabelType","IsDecoy")) %>%
      filter(Keep=="keep") %>% select(-Keep)%>%
      #select(ID_Analyte,IsotopeLabelType,ID_FragmentIon_charge,ID_Rep,InterpolatedTimes,
      #       InterpolatedIntensities,InterpolatedMassErrors,Area,LibraryIntensity,MinStartTime,MaxEndTime)%>%
      arrange(ID_Analyte,ID_FragmentIon_charge,ID_Rep)
  } else {
    
    ## Filter less than MinimalInitialNumberOfTransitions_Filter transitions
    FilterLessThanNTrans<-D1 %>%
      select(ProteinName,PeptideModifiedSequence,PrecursorCharge,IsotopeLabelType,FragmentIon,ProductCharge,IsDecoy) %>%
      distinct()  %>%
      group_by(ProteinName,PeptideModifiedSequence,PrecursorCharge,IsotopeLabelType,IsDecoy) %>%
      tally() %>%
      mutate(Keep=ifelse(n>=MinimalInitialNumberOfTransitions_Filter,"keep","DoNOTKeep")) %>%
      select(ProteinName,PeptideModifiedSequence,PrecursorCharge,IsotopeLabelType,IsDecoy,Keep)
    
    D1<- D1 %>%
      left_join(data.frame(FilterLessThanNTrans),by = c("ProteinName","PeptideModifiedSequence", "PrecursorCharge", "IsotopeLabelType","IsDecoy")) %>%
      filter(Keep=="keep") %>% select(-Keep)%>%
      #select(ID_Analyte,IsotopeLabelType,ID_FragmentIon_charge,ID_Rep,InterpolatedTimes,
      #       InterpolatedIntensities,InterpolatedMassErrors,Area,LibraryIntensity,MinStartTime,MaxEndTime)%>%
      arrange(ID_Analyte,ID_FragmentIon_charge,ID_Rep)
  }
  
  return(D1)}
data_loader_from_PartitionedParquet<-function(D){
  # Loading the data
  
  
  Chrom.Analyte = D %>% distinct()
  
  
  ### Boundaries
  Boundaries<-tapply(paste(Chrom.Analyte$ID_FragmentIon_charge,Chrom.Analyte$MinStartTime,Chrom.Analyte$MaxEndTime,Chrom.Analyte$InterpolatedTimes,sep=','), paste0("Rep_",Chrom.Analyte$ID_Rep," Analyte_",Chrom.Analyte$ID_Analyte," IsotopeLabelType_",Chrom.Analyte$IsotopeLabelType), function(x){
    m=strsplit(x, ',') %>% unlist() %>% gsub(pattern=' *', replacement='') %>% matrix(nrow=length(x), byrow=T)
    m=t(m)
    m=m[-1,1]
    n=ifelse(round(as.numeric(m),5)>=round(as.numeric(m[1]),5) & round(as.numeric(m),5)<=round(as.numeric(m[2]),5),1,0)
    m=cbind(m,n)
    m=m[-c(1,2),]
    
    # Find replicate where only a single data point was integrated.
    o= as.numeric(m[,dim(m)[2]])  
    if(sum(o)==1) {
      # ^ If there is only one data point integarted then another one is added to 
      # avoid the autamatic conversion of single-row dataframes into vectors.
      r = ifelse(which(o==1)==1, which(o==1)+1, which(o==1)-1)
      # ^ Thishandles the case where the single data point is the first row of the dataframe,
      # then it adds the second row. If not, it adds the previous row.
      m[r, dim(m)[2]] = 1
    }
    
    colnames(m)=c("Times","IntegrationZone")
    m
  })
  
  ##### Chromatograms
  
  Chrom_Full<-tapply(paste(Chrom.Analyte$ID_FragmentIon_charge,
                           Chrom.Analyte$InterpolatedIntensities,sep=','),
                     paste0("Rep_",Chrom.Analyte$ID_Rep," Analyte_",
                            Chrom.Analyte$ID_Analyte," IsotopeLabelType_",Chrom.Analyte$IsotopeLabelType),
                     function(x){
                       m=strsplit(x, ',') %>% unlist() %>% gsub(pattern=' *', replacement='') %>% matrix(nrow=length(x), byrow=T)
                       m=t(m)
                       
                       colnames(m) <- m[1,]
                       m=m[-1,]
                       m= apply(m,2,as.numeric) 
                       
                       row.names(m) <- paste('Point', 1:nrow(m), sep='.')
                       m
                     })
  
  Chrom_Full<-rapply( Chrom_Full, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
  Chrom_Full<-rapply( Chrom_Full, f=function(x) ifelse((x)<0,0,x), how="replace" )
  
  Chrom<-Map(cbind, Chrom_Full,Boundaries)
  Chrom<-lapply(X = Chrom,FUN = function(W){
    W=W[(W[,which(colnames(W)=="IntegrationZone")]==1), ,drop=FALSE]
    W=W[,1:(dim(W)[2]-2), drop=FALSE]
  })
  
  ##### Normalized Chromatograms
  
  Norm.Chrom<-lapply(Chrom,FUN = function(m){
    p=row.names(m)
    m= apply(m,2,as.numeric)
    m= apply(m, 2, function(x2)x2/max(x2, na.rm=T))*100
    row.names(m) <- p
    m
  })
  
  ##### Mass.Errors
  
  MassErrors_Full<-tapply(paste(Chrom.Analyte$ID_FragmentIon_charge,Chrom.Analyte$InterpolatedMassErrors,sep=','), paste0("Rep_",Chrom.Analyte$ID_Rep," Analyte_",Chrom.Analyte$ID_Analyte," IsotopeLabelType_",Chrom.Analyte$IsotopeLabelType), function(x){
    m=strsplit(x, ',') %>% unlist() %>% gsub(pattern=' *', replacement='') %>% matrix(nrow=length(x), byrow=T)
    m=t(m)
    colnames(m) <- m[1,]
    m=m[-1,]
    m= apply(m,2,as.numeric) 
    row.names(m) <- paste('Point', 1:nrow(m), sep='.')
    m
  })
  
  MassErrors_Full<-rapply( MassErrors_Full, f=function(x) ifelse(is.nan(x),0,x), how="replace" )
  MassErrors_Full<-Map(cbind, MassErrors_Full,Boundaries)
  ### Do not change the order of these operations!!!
  MassErrors<-lapply(X = MassErrors_Full,FUN = function(W){
    W=W[(W[,which(colnames(W)=="IntegrationZone")]==1),]
    W=W[,1:(dim(W)[2]-2)]
  })
  
  MassErrors<-Map(rbind,MassErrors,Norm.Chrom)
  colnames(MassErrors[[1]])<-ifelse(duplicated(colnames(MassErrors[[1]])),paste0("W_",colnames(MassErrors[[1]])),colnames(MassErrors[[1]]))
  
  
  MassErrors<-lapply(MassErrors,FUN =function(L){
    a=dim(L)[1]/2
    b=dim(L)[1]
    meanMassErrors.IntegratedZone<-apply(L,2,function(x){
      y=abs(as.numeric(x[1:a]))
      z=as.numeric(x[(a+1):b])
      weighted.mean(y,z,na.rm = T)
    })
    SkorMassErrors.IntegratedZone<-ifelse(as.numeric(meanMassErrors.IntegratedZone)<=MassError_Tolerance,1,ifelse(as.numeric(meanMassErrors.IntegratedZone)<=MassError_CutOff,MassError_CutOff/(MassError_CutOff-MassError_Tolerance)+as.numeric(meanMassErrors.IntegratedZone)*(1/(MassError_Tolerance-MassError_CutOff)),0))
    t(data.frame(meanMassErrors.IntegratedZone,SkorMassErrors.IntegratedZone))
  })  
  
  
  ## Library Intensities
  SpctLib <- Chrom.Analyte %>% 
    select(Area,LibraryIntensity, ID_Rep,ID_Analyte,IsotopeLabelType,ID_FragmentIon_charge) %>% 
    filter(ID_Rep== unique(ID_Rep)[1]) %>% filter(IsotopeLabelType== unique(IsotopeLabelType)[1]) %>% 
    mutate(Rank=rank(-LibraryIntensity,ties.method= "min")) %>% distinct()
  Transition.Lib.Intensity=as.matrix(t(SpctLib[,"LibraryIntensity"]))
  colnames(Transition.Lib.Intensity)=SpctLib[,"ID_FragmentIon_charge"]
  row.names(Transition.Lib.Intensity)="Lib.Intensity"
  
  ## Rank transitions by  Spectral Library Intensity
  Transition.Rank=as.matrix(t(SpctLib[,"Rank"]))
  colnames(Transition.Rank)=SpctLib[,"ID_FragmentIon_charge"]
  row.names(Transition.Rank)="Rank"
  
  ## DIA Area  per transition
  
  Transition.Area<- Chrom.Analyte %>%
    select(ID_FragmentIon_charge,Area,ID_Rep,ID_Analyte,IsotopeLabelType) %>% 
    mutate(Area = as.numeric(Area)) %>%
    distinct %>%
    group_by(ID_Rep,ID_Analyte,IsotopeLabelType) %>%
    spread(key = ID_FragmentIon_charge,value = Area)  %>% ungroup### Normalized DIA.Areas
  
  Transition.Area<- split(as.data.frame(Transition.Area,stringsAsFactors = F), f =paste0("Analyte_",Transition.Area$ID_Analyte," IsotopeLabelType_",Transition.Area$IsotopeLabelType), drop=FALSE) 
  
  Transition.Area<- lapply(Transition.Area,FUN = function(L) {
    L=L %>%
      select(-ID_Analyte,-IsotopeLabelType) %>%
      ungroup
    
    row.names(L)=paste0("Rep_",L$ID_Rep)
    
    L=L %>%select(-ID_Rep)
    return(L)})
  
  MPRA.MeanArea<-lapply(data.matrix(Transition.Area), FUN = function(L) {
    P=data.frame(t(apply(L,2, mean, na.rm=T)),stringsAsFactors = F)
    names(P)=gsub(names(P),pattern = "X",replacement = "")
    row.names(P)=c("Mean.Area")
    return(P)})
  
  #### Replicate-wide data
  
  for(i in 1:length(names(Transition.Area))){
    Transition.Area[[i]]=rbind(MPRA.MeanArea[[i]],Transition.Lib.Intensity,Transition.Area[[i]])
  }
  
  
  Transition.Area= lapply(Transition.Area,FUN = function(L){ MAX.L=apply(L,1,max,na.rm =T)
  SUM.L=apply(L,1,sum,na.rm =T)
  L=cbind(L,Total.To.Max.Ratio=SUM.L/MAX.L)})
  
  
  ### Chromatogram_Score
  for(i in 1:length(names(Chrom_Full))){
    Chrom_Full[[i]]=rbind(MPRA.MeanArea[[1]],Transition.Lib.Intensity,Chrom_Full[[i]])
  }
  
  return(list(Chrom.Analyte=Chrom.Analyte,
              Boundaries=Boundaries,
              Chrom_Full=Chrom_Full,
              Chrom=Chrom,
              Norm.Chrom=Norm.Chrom,
              MassErrors_Full=MassErrors_Full,
              MassErrors=MassErrors,
              Transition.Area=Transition.Area,
              Transition.Rank=Transition.Rank))
}
Run_Transition_Refinment_Tool_modif<-function(Chrom.Analyte,Norm.Chrom,MassErrors,Transition.Area,Transition.Rank){
  
  # %%%%%%%%%%%%% Parameters
  
  num_trans=dim(Norm.Chrom[[1]])[2]
  
  if (num_trans<MinimalNumberOfTransitionsAfterOptimization) {
    if (KeepPeptidesWithLowerNumberOfTransitions==TRUE) {
      Report.Replicate.Values<-Report.Replicate_informedMPRA(Norm.ChromX = Norm.Chrom,Transition.Area=Transition.Area,MassErrorsX = MassErrors,y = rep(1,num_trans), Comment ="aa-NonOptimized_LowerNumOfMinTrans")
      
      Report.Transition.Values<-Report.Transition(Chrom.AnalyteX = Chrom.Analyte,Norm.ChromX = Norm.Chrom,y = rep(1,num_trans))
      
      return(list(Report.Transition.Values=Report.Transition.Values,
                  Report.Replicate.Values=Report.Replicate.Values))
    } else{return(0)}
  } else{
    
    num_rep=dim(Transition.Area[[1]])[1]-2
    sug=rep(1,num_trans)
    Trans.Vector=colnames(Norm.Chrom[[1]])
    
    #     Functions
    
    Trans.Classified<-Transition.Classifier(Norm.Chrom = Norm.Chrom,Transition.Area = Transition.Area,num_trans = num_trans,Transition.Rank = Transition.Rank)
    sug.matrix<-Sug.Matrix.FUN(Trans.Classified,Trans.Vector = Trans.Vector)
    
    
    
    Transitions.To.Remove= data.frame(
      Trans.Classified %>% 
        select(ID_FragmentIon_charge,Rank.The25quartile.Initial.SimilarityScore,Rank.The25quartile.Initial.Area,Rank) %>%
        arrange(-Rank.The25quartile.Initial.SimilarityScore,-Rank) %>%
        select(ID_FragmentIon_charge))[c(0:num.trans.To.Remove.Fun(num_trans)),]
    ### Remove transition if the median mass error is higher than the mass error cutoff
    Transitions_To_Remove_Mass_Error<-data.frame(rbindlist(lapply(MassErrors,FUN = function(L){data.frame(Transitions=colnames(L),SkorMassErrors_IntegratedZone=L[1,])})))
    
    Transitions_To_Remove_Mass_Error<-Transitions_To_Remove_Mass_Error %>% group_by(Transitions) %>%
      summarise(median_error=median(SkorMassErrors_IntegratedZone,na.rm = T)) %>%
      arrange(-median_error) %>%
      filter(median_error>=MassError_CutOff) %>%
      select(Transitions) %>%
      mutate(Transitions=as.character(Transitions))
    
    Transitions.To.Remove<-unique(c(Transitions.To.Remove,Transitions_To_Remove_Mass_Error$Transitions))
    #
    
    
    Transitions.To.Remove=sapply(Trans.Vector,function(x) ifelse(x %in% Transitions.To.Remove,0,1))
    
    
    
    
    Trans.Classified_filtered<-Trans.Classified[Trans.Classified$ID_FragmentIon_charge %in% names(Transitions.To.Remove[Transitions.To.Remove==1]),]
    Trans.Vector2= names(Transitions.To.Remove[Transitions.To.Remove==1])
    num_trans2=length(Trans.Vector2)
    
    
    #########  New dataset without the worst transitions
    
    Norm.Chrom2<-Transition.Remover(Norm.Chrom,Transitions.To.Remove)
    
    Transition.Area2<-Transition.Remover(Transition.Area,Transitions.To.Remove)
    Transition.Area2=lapply(Transition.Area2,FUN = function(L){ MAX.L=apply(L,1,max,na.rm =T);SUM.L=apply(L,1,sum,na.rm =T); L=cbind(L,Total.To.Max.Ratio=SUM.L/MAX.L)})
    
    Transition.Rank2=t(matrix(Transition.Rank[which(Transitions.To.Remove==1)]))
    colnames(Transition.Rank2)=colnames(Transition.Rank)[which(Transitions.To.Remove==1)]
    row.names(Transition.Rank2)="Rank"
    
    MassErrors2=Transition.Remover(MassErrors,Transitions.To.Remove)
    
    sug.matrix2<-Sug.Matrix.FUN(Trans.Classified_filtered,Trans.Vector = Trans.Vector2)
    
    
    GA_TransitionOpt<-ga(type ="binary",
                         fitness = GA.Fitness,
                         Norm.Chrom=Norm.Chrom2,
                         Transition.Area=Transition.Area2,
                         Transition.Rank=Transition.Rank2,
                         MassErrors=MassErrors2,
                         nBits = num_trans2, 
                         suggestions = sug.matrix2, 
                         maxiter=10,run=3, 
                         popSize=ifelse(dim(sug.matrix2)[1]<=20,20,dim(sug.matrix2)[1]),
                         pcrossover=0.9,
                         pmutation=0.1,
                         elitism=0.1, 
                         keepBest = T,
                         seed = 112358)
    
    
    GA_1stRunSolution=Trans.Vector2[which(GA_TransitionOpt@solution[1,]==1)]
    
    ## {{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{{}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}}
    
    Trans.Classified2<-Transition.Classifier2(Norm.Chrom = Norm.Chrom2,Transition.Area = Transition.Area2,num_trans = num_trans2,y = GA_TransitionOpt@solution[1,],Transition.Rank = Transition.Rank)
    
    Transitions.To.Remove2= data.frame(
      Trans.Classified2 %>% 
        select(ID_FragmentIon_charge,Rank.The25quartile.Initial.SimilarityScore,Rank.The25quartile.Initial.Area,Rank) %>%
        arrange(-Rank.The25quartile.Initial.SimilarityScore,-Rank) %>%
        select(ID_FragmentIon_charge))[c(0:num.trans.To.Remove.Fun(num_trans2)),]
    
    Transitions.To.Remove2=sapply(Trans.Vector2,function(x) ifelse(x %in% Transitions.To.Remove2,0,1))
    
    Trans.Classified_filtered2<-Trans.Classified2[Trans.Classified2$ID_FragmentIon_charge %in% names(Transitions.To.Remove2[Transitions.To.Remove2==1]),]
    
    Trans.Vector3= names(Transitions.To.Remove2[Transitions.To.Remove2==1])
    num_trans3=length(Trans.Vector3)
    
    
    #########  New dataset without the worst transitions
    
    Norm.Chrom3<-Transition.Remover(Norm.Chrom2,Transitions.To.Remove2)
    
    Transition.Area3<-Transition.Remover(Transition.Area2,Transitions.To.Remove2)
    Transition.Area3=lapply(Transition.Area3,FUN = function(L){ MAX.L=apply(L,1,max,na.rm =T);SUM.L=apply(L,1,sum,na.rm =T); L=cbind(L,Total.To.Max.Ratio=SUM.L/MAX.L)})
    
    Transition.Rank3=t(matrix(Transition.Rank2[which(Transitions.To.Remove2==1)]))
    colnames(Transition.Rank3)=colnames(Transition.Rank2)[which(Transitions.To.Remove2==1)]
    row.names(Transition.Rank3)="Rank"
    
    MassErrors3=Transition.Remover(MassErrors2,Transitions.To.Remove2)
    
    sug.matrix3<-Sug.Matrix.FUN(Trans.Classified_filtered2,Trans.Vector = Trans.Vector3)
    
    sug.matrix3<-rbind(sug.matrix3,ifelse(Trans.Vector3 %in% GA_1stRunSolution,1,0))
    
    GA_TransitionOpt_2<-ga(type ="binary",
                           fitness = GA.Fitness,
                           Norm.Chrom=Norm.Chrom3,
                           Transition.Area=Transition.Area3,
                           Transition.Rank=Transition.Rank3,
                           MassErrors=MassErrors3,
                           nBits = num_trans3, 
                           suggestions = sug.matrix3, 
                           maxiter=10,run=5, 
                           popSize=ifelse(dim(sug.matrix2)[1]<=20,20,dim(sug.matrix2)[1]),
                           pcrossover=0.9,
                           pmutation=0.1,
                           elitism=0.1, 
                           keepBest = T,
                           seed = 112358)
    
    
    Report.Replicate.Values<-rbind(Report.Replicate_informedMPRA_modif(Norm.ChromX = Norm.Chrom,Transition.Area=Transition.Area,MassErrorsX = MassErrors,y = rep(1,num_trans), Comment ="a-Initial"),
                                   Report.Replicate_informedMPRA_modif(Norm.ChromX = Norm.Chrom2,Transition.Area=Transition.Area2,MassErrorsX = MassErrors2,y = rep(1,num_trans2), Comment ="b-After 1st similarity score filter"),
                                   Report.Replicate_informedMPRA_modif(Norm.ChromX = Norm.Chrom2,Transition.Area=Transition.Area2,MassErrorsX = MassErrors2,y = GA_TransitionOpt@solution[1,], Comment ="c-After 1st GA optimization"),
                                   Report.Replicate_informedMPRA_modif(Norm.ChromX = Norm.Chrom3,Transition.Area=Transition.Area3,MassErrorsX = MassErrors3,y = rep(1,num_trans3), Comment ="d-After 2nd similarity score filter"),
                                   Report.Replicate_informedMPRA_modif(Norm.ChromX = Norm.Chrom3,Transition.Area=Transition.Area3,MassErrorsX = MassErrors3,y = GA_TransitionOpt_2@solution[1,],Comment = "e-After 2nd GA optimization"))
    
    Report.Transition.Values<-Report.Transition_modif(Chrom.AnalyteX = Chrom.Analyte,Norm.ChromX = Norm.Chrom3,y = GA_TransitionOpt_2@solution[1,])
    
    return(list(Report.Transition.Values=Report.Transition.Values,
                Report.Replicate.Values=Report.Replicate.Values))}
}
Report.Replicate_informedMPRA_modif<-function(Norm.ChromX,Transition.AreaX,MassErrorsX,y,Comment){
  #Similarity Scores  
  H1<-Similarity.Score.Report_informsMPRA(Norm.ChromX,y)
  H=H1$Report
  H<-data.frame(H)%>%
    mutate(ID_FragmentIon_charge=row.names(H)) %>%
    gather(key = "Rep",value = "Similarity.Score",1:(dim(H)[2])) %>%
    group_by(Rep) %>% summarise(Similarity.Score=mean(Similarity.Score))
  H<-data.frame(H) %>% mutate(Rep=paste0(Rep,".")) %>% 
    mutate(Rep2=substr(Rep,start = str_locate(Rep,pattern = "Rep_"),stop = nchar(Rep)),
           Analyte=substr(Rep,start = str_locate(Rep,pattern = "Analyte_"),stop = nchar(Rep)),
           IsotopeLabelType=substr(Rep,start = str_locate(Rep,pattern = "IsotopeLabelType_"),stop = nchar(Rep))) %>%
    mutate(Rep2=substr(Rep2,stop = str_locate(Rep2,pattern = "\\.")-1,start = 5),
           Analyte=substr(Analyte,stop = str_locate(Analyte,pattern = "\\.")-1,start = 9),
           IsotopeLabelType=substr(IsotopeLabelType,stop = str_locate(IsotopeLabelType,pattern = "\\.")-1,start = 18)) %>%
    rename(ID_Rep=Rep2, ID_Analyte=Analyte) %>%
    select(-Rep) %>%
    mutate(ID_Rep = as.character(ID_Rep))
  
  K0<-MassError.Score.Report(MassErrors = MassErrorsX,y = y)
  K1=as.character(row.names(K0$Report))
  K<- data.frame(K0$Report) %>% mutate(Rep=K1) %>% 
    mutate(Rep=paste0(Rep,".")) %>% 
    mutate(Rep=gsub(Rep,pattern=" ",replacement=".")) %>% 
    mutate(Rep2=substr(Rep,start = str_locate(Rep,pattern = "Rep_"),stop = nchar(Rep)),
           Analyte=substr(Rep,start = str_locate(Rep,pattern = "Analyte_"),stop = nchar(Rep)),
           IsotopeLabelType=substr(Rep,start = str_locate(Rep,pattern = "IsotopeLabelType_"),stop = nchar(Rep))) %>%
    mutate(Rep2=substr(Rep2,stop = str_locate(Rep2,pattern = "\\.")-1,start = 5),
           Analyte=substr(Analyte,stop = str_locate(Analyte,pattern = "\\.")-1,start = 9),
           IsotopeLabelType=substr(IsotopeLabelType,stop = str_locate(IsotopeLabelType,pattern = "\\.")-1,start = 18)) %>%
    rename(ID_Rep=Rep2, ID_Analyte=Analyte) %>% select(-Rep) %>% rename(Score.MassError=K0.Report) %>%
    mutate(ID_Rep = as.character(ID_Rep))
  
  
  
  
  I<-MPRA_y_SpectLib.Report_informed(Transition.Area = Transition.AreaX,y = y,Best_reps = H1$Best_reps,K0$Best_reps_MassErrors)
  
  MPRA<-data.frame(Rep=row.names(I$MPRA.score),I$MPRA.score)
  MPRA<-MPRA%>% gather(key = "Rep2",value = "MPRA.Score",2:dim(MPRA)[2]) %>%
    mutate(Rep=paste0(Rep,".",Rep2)) %>% select(-Rep2)
  
  Lib.dotp<-data.frame(Rep=row.names(I$Library.dotp),I$Library.dotp)
  Lib.dotp<-Lib.dotp%>% gather(key = "Rep2",value = "Library.dotp",2:dim(Lib.dotp)[2]) %>%
    mutate(Rep=paste0(Rep,".",Rep2)) %>% select(-Rep2)
  
  I<-left_join(MPRA,Lib.dotp,by="Rep") %>% mutate(Rep=paste0(Rep,".")) %>% mutate(Rep=substr(Rep,start = str_locate(Rep,pattern = "Rep_"),stop = nchar(Rep)),
                                                                                  Analyte=substr(Rep,start = str_locate(Rep,pattern = "Analyte_"),stop = nchar(Rep)),
                                                                                  IsotopeLabelType=substr(Rep,start = str_locate(Rep,pattern = "IsotopeLabelType_"),stop = nchar(Rep))
  ) %>%
    mutate(Rep=substr(Rep,stop = str_locate(Rep,pattern = "\\.")-1,start = 5),
           Analyte=substr(Analyte,stop = str_locate(Analyte,pattern = "\\.")-1,start = 9),
           IsotopeLabelType=substr(IsotopeLabelType,stop = str_locate(IsotopeLabelType,pattern = "\\.")-1,start = 18)) %>%
    rename(ID_Rep=Rep, ID_Analyte=Analyte) %>%
    mutate(ID_Rep = as.character(ID_Rep))
  
  
  
  J<-Intensity.Fitness.Report(Transition.Area = Transition.AreaX,y=y)
  J<- data.frame(J) %>% mutate(Rep=row.names(J)) %>% 
    mutate(Rep=paste0(Rep,".")) %>% 
    mutate(Rep=gsub(Rep,pattern=" ",replacement=".")) %>% 
    mutate(Rep2=substr(Rep,start = str_locate(Rep,pattern = "Rep_"),stop = nchar(Rep)),
           Analyte=substr(Rep,start = str_locate(Rep,pattern = "Analyte_"),stop = nchar(Rep)),
           IsotopeLabelType=substr(Rep,start = str_locate(Rep,pattern = "IsotopeLabelType_"),stop = nchar(Rep))) %>%
    mutate(Rep2=substr(Rep2,stop = str_locate(Rep2,pattern = "\\.")-1,start = 5),
           Analyte=substr(Analyte,stop = str_locate(Analyte,pattern = "\\.")-1,start = 9),
           IsotopeLabelType=substr(IsotopeLabelType,stop = str_locate(IsotopeLabelType,pattern = "\\.")-1,start = 18)) %>%
    rename(ID_Rep=Rep2, ID_Analyte=Analyte) %>% select(-Rep) %>%
    mutate(ID_Rep = as.character(ID_Rep))
  
  
  
  
  Report<-H %>% left_join(I,by = c("ID_Rep","ID_Analyte","IsotopeLabelType")) %>%
    left_join(J,by = c("ID_Rep","ID_Analyte","IsotopeLabelType")) %>%
    left_join(K,by = c("ID_Rep","ID_Analyte","IsotopeLabelType")) %>%
    select(ID_Analyte,IsotopeLabelType,ID_Rep,Similarity.Score,MPRA.Score,Library.dotp,Intensity.Score,Score.MassError) %>% mutate(Comment=Comment)
  
  
  return(Report)
}
Report.Transition_modif<-function(Chrom.AnalyteX,Norm.ChromX,y){
  #Similarity Scores  
  SimS<-Similarity.Score.Report_informsMPRA(Norm.ChromX,y)
  H<-SimS$Report
  H<-data.frame(H)%>%
    mutate(ID_FragmentIon_charge=row.names(H)) %>%
    gather(key = "Rep",value = "Similarity.Score",1:(dim(H)[2]))
  H<- data.frame(H)%>% mutate(Rep=paste0(Rep,".")) %>% 
    mutate(Rep=substr(Rep,start = str_locate(Rep,pattern = "Rep_"),stop = nchar(Rep)),
           Analyte=substr(Rep,start = str_locate(Rep,pattern = "Analyte_"),stop = nchar(Rep)),
           IsotopeLabelType=substr(Rep,start = str_locate(Rep,pattern = "IsotopeLabelType_"),stop = nchar(Rep))) %>%
    mutate(Rep=substr(Rep,stop = str_locate(Rep,pattern = "\\.")-1,start = 5),
           Analyte=substr(Analyte,stop = str_locate(Analyte,pattern = "\\.")-1,start = 9),
           IsotopeLabelType=substr(IsotopeLabelType,stop = str_locate(IsotopeLabelType,pattern = "\\.")-1,start = 18)) %>%
    rename(ID_Rep=Rep, ID_Analyte=Analyte) %>% 
    mutate(ID_Rep=as.character(ID_Rep),ID_Analyte=as.character(ID_Analyte))
  Y<-Chrom.AnalyteX %>% select(ID_Rep,ID_Analyte,IsotopeLabelType) %>%
    distinct()
  H<-H %>%left_join(Y, by = c("ID_Rep","ID_Analyte","IsotopeLabelType"))
  return(H)
}
AvantGardeDIA_GlobalRefinement_modif<-function(D){
  AG<-data_loader_from_PartitionedParquet(D = D)
  Chrom.Analyte=AG$Chrom.Analyte
  Boundaries=AG$Boundaries
  Chrom_Full=AG$Chrom_Full
  Chrom=AG$Chrom
  Norm.Chrom=AG$Norm.Chrom
  MassErrors_Full=AG$MassErrors_Full
  MassErrors=AG$MassErrors
  Transition.Area=AG$Transition.Area
  Transition.Rank=AG$Transition.Rank
  rm(AG)
  
  
  ########### Running the tools
  Results_TransitionRefinementTool<-Run_Transition_Refinment_Tool_modif(Chrom.Analyte = Chrom.Analyte,
                                                                        Norm.Chrom = Norm.Chrom,
                                                                        MassErrors = MassErrors,
                                                                        Transition.Area = Transition.Area,
                                                                        Transition.Rank = Transition.Rank)
  TransitionRefinementSolution<-unique(Results_TransitionRefinementTool$Report.Transition.Values$ID_FragmentIon_charge)
  
  Results_PeakBoundaries_tool<-Run_PeakBoundaries_tool(Chrom_Full,MassErrors_Full,TransitionRefinementSolution,Boundaries)
  
  Results_ReScore<-Run_Rescoring_Tool(Chrom.AnalyteX = Chrom.Analyte,
                                      TransitionRefinementSolution =TransitionRefinementSolution,
                                      Results_PeakBoundaries_tool=Results_PeakBoundaries_tool,Comment = "f.GlobalRefinement")
  ColNames_Report=NULL
  ColNames_Report[1]=paste0(names(Results_TransitionRefinementTool$Report.Transition.Values),collapse = ";")
  ColNames_Report[2]=paste0(names(Results_TransitionRefinementTool$Report.Replicate.Values),collapse = ";")
  ColNames_Report[3]=paste0(names(Results_PeakBoundaries_tool$New_PeakBoundaries),collapse = ";")
  return(list(Results_TransitionRefinementTool=Results_TransitionRefinementTool,
              Results_PeakBoundaries_tool=Results_PeakBoundaries_tool,
              Results_ReScore=Results_ReScore,
              ColNames_Report=ColNames_Report))
  
}


source(params_file)

data= fread(file=analyte_data,
            header =T, stringsAsFactors = F)
names(data)<-gsub(names(data),pattern = "\\.",replacement = "")
names(data)<-gsub(names(data),pattern = " ",replacement = "")

data <- data %>% filter(!is.na(LibraryIntensity))
data <- Filter_Na_Shared_Or_LowMassTransitions(data)

i = analyte_hash_id
A<-AvantGardeDIA_GlobalRefinement_modif(data)

transition_results <- A$Results_TransitionRefinementTool$Report.Transition.Values %>% select(ID_FragmentIon_charge,ID_Analyte) %>% distinct()
peak_Boundaries_results <- A$Results_PeakBoundaries_tool$New_PeakBoundaries %>% select(MinStartTime=left,MaxEndTime=right, ID_Rep, ID_Analyte) %>% distinct()
write.table(transition_results, file=paste0(output_dir,"/Report_GR_Transitions_",Name_Tag,"_",i,".csv"),quote=F,row.names=F,col.names=F,sep=";")
write.table(peak_Boundaries_results,file=paste0(output_dir,"/Report_GR_PeakBoundaries_",Name_Tag,"_",i,".csv"),quote=F,row.names=F,col.names=F,sep=";")
