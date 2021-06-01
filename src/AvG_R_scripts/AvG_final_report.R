.libPaths(c( "C:/Users/svaca/Documents/R/win-library/4.0", "I:/R/win-library/4.0"))


print(.libPaths())

library(dplyr)
library(tidyr)
library(stringr)
library(data.table)

change_names<-function(data){
  names(data)<-gsub(names(data),pattern = "\\.",replacement = "")
  names(data)<-gsub(names(data),pattern = " ",replacement = "")
  return(data)
}

args <- commandArgs(TRUE)
print(args)

params_file <- as.character(args[1])
avg_results_path <- as.character(args[2])
MetaData_Analytes_path <- as.character(args[3])
Transition_Locator <- as.character(args[4])
MetaData_Replicate <- as.character(args[5])
MetaData_PrecursorResults <- as.character(args[6]) ##new
output_path <- as.character(args[7])
success_file <- as.character(args[8])

# 
# params_file <- "C:/Users/svaca/Documents/AvG_Example/ExampleAvG.sky/AvantGardeDIA/AvG_Params.R"
# avg_results_path<- "C:/Users/svaca/Documents/Code_cdrive/phosphoDIA_Pipeline/data2/avg_results/"
# MetaData_Analytes<- fread("C:/Users/svaca/Documents/Code_cdrive/phosphoDIA_Pipeline/data2/avg_results/ID_Analyte_glossary_3_0b4f59c7.csv",
#                           stringsAsFactors = F) %>% change_names()
# Transition_Locator<- fread("C:/Users/svaca/Documents/Code_cdrive/phosphoDIA_Pipeline/data2/csv_ds/ID_transition_locator.csv",
#                           stringsAsFactors = F) %>% change_names()
# MetaData_Replicate<- fread("C:/Users/svaca/Documents/Code_cdrive/phosphoDIA_Pipeline/data2/csv_ds/ID_Rep.csv",
#                            stringsAsFactors = F) %>% change_names()
# output_path<- "C:/Users/svaca/Documents/Code_cdrive/phosphoDIA_Pipeline/data2/final_result2/"
# MetaData_PrecursorResults<-fread("C:/Users/svaca/Documents/Code_cdrive/phosphoDIA_Pipeline/data2/csv_ds/MetaData_PrecursorResults.csv",
#                                  stringsAsFactors = F) %>% change_names()
  
MetaData_Analytes<- fread(MetaData_Analytes_path, stringsAsFactors = F) %>% change_names()
Transition_Locator<- fread(Transition_Locator, stringsAsFactors = F) %>% change_names()
MetaData_Replicate<- fread(MetaData_Replicate, stringsAsFactors = F) %>% change_names()
MetaData_PrecursorResults<- fread(MetaData_PrecursorResults, stringsAsFactors = F) %>% change_names()##new

source(params_file)
## Peak_BOundaries
PBlist<-list.files(avg_results_path, pattern = paste0("Report_GR_PeakBoundaries_"))
ListFiles_PeakBoundaries<-paste0(avg_results_path,"/",PBlist)

if(length(PBlist)>=1){
  NewPeakBoundaries_All_Results <- lapply(ListFiles_PeakBoundaries, fread, header = F,sep=';', stringsAsFactors = FALSE)
  NewPeakBoundaries_All_Results <- rbindlist( NewPeakBoundaries_All_Results )
  
  colnames(NewPeakBoundaries_All_Results)<-c("MinStartTime","MaxEndTime", "ID_Rep", "ID_Analyte")
  
  NewPeakBoundaries_All_Results<-NewPeakBoundaries_All_Results %>%
    mutate(MinStartTime=ifelse(is.na(MinStartTime),"#N/A",MinStartTime),
           MaxEndTime=ifelse(is.na(MaxEndTime),"#N/A",MaxEndTime)) %>%
    mutate(ID_Analyte=as.character(ID_Analyte),
           ID_Rep=as.character(ID_Rep)) %>%
    left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
    left_join(MetaData_Replicate, by = c("ID_Rep"))
  
  
  NewPeakBoundaries_All_Results<-NewPeakBoundaries_All_Results %>%
    select(FileName,PeptideModifiedSequence,MinStartTime,MaxEndTime,PrecursorCharge,IsDecoy) %>%
    rename(PrecursorIsDecoy=IsDecoy) %>%
    distinct()
  
  write.csv(NewPeakBoundaries_All_Results,file=paste0(output_path,"Peak_Boundaries_results.csv"),quote=F,row.names=F)
  rm(NewPeakBoundaries_All_Results)
}

## Report Transitions
Translist<-list.files(avg_results_path,pattern = paste0("Report_GR_Transitions_"))
ListFiles_Transitions<-paste0(avg_results_path,"/",Translist)
if(length(Translist)>=1){
  NewTransitions_All_Results <- lapply(ListFiles_Transitions, fread, header = F,sep=';', stringsAsFactors = FALSE)
  NewTransitions_All_Results <- rbindlist( NewTransitions_All_Results )
  colnames(NewTransitions_All_Results)<-c("ID_FragmentIon_charge","ID_Analyte")
  
  NewTransitions_All_Results<-NewTransitions_All_Results %>% mutate(Quantitative= "TRUE")
  
  NewTransitions_All_Results<-Transition_Locator %>%
    full_join(NewTransitions_All_Results, by = c("ID_FragmentIon_charge", "ID_Analyte")) %>%
    mutate(Quantitative= ifelse(is.na(Quantitative),FALSE,TRUE)) %>%
    rename(ElementLocator = TransitionLocator) %>%
    select(ElementLocator, Quantitative)
  
  write.table(NewTransitions_All_Results,file=paste0(output_path,"Transition_results.csv"),quote=F,row.names=F,col.names=T,sep=",")
  rm(NewTransitions_All_Results)
}


write.table(
  fread(MetaData_Analytes_path, stringsAsFactors = F), file = success_file, quote=F,row.names=F,col.names=T,sep=",")

## Report Replicates
RepList<-list.files(avg_results_path,pattern = paste0("Report_GR_Replicate_"))
ListFiles_Replicates<-paste0(avg_results_path,"/",RepList)
if(length(RepList)>=1){
  NewReplicates_All_Results <- lapply(ListFiles_Replicates, fread, header = F,sep=';', stringsAsFactors = FALSE)
  NewReplicates_All_Results <- rbindlist( NewReplicates_All_Results )
  colnames(NewReplicates_All_Results)<-c("ID_Analyte","IsotopeLabelType" , "ID_Rep",  "Similarity.Score" , "MPRA.Score" , "Library.dotp",  "Intensity.Score", "Score.MassError", "Comment")
  
  NewReplicates_All_Results<-NewReplicates_All_Results %>%
    mutate(ID_Analyte=as.character(ID_Analyte),
           ID_Rep=as.character(ID_Rep)) %>%
    left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
    left_join(MetaData_Replicate, by = c("ID_Rep"))
  
  Exponents<-c(9.5,4.5,2.5,0.5)
  NewReplicates_All_Results<-NewReplicates_All_Results %>%
    mutate(Skor=Similarity.Score^Exponents[1]*Library.dotp^Exponents[2]*Score.MassError^Exponents[3]*MPRA.Score^Exponents[4])
  
  write.table(NewReplicates_All_Results,file=paste0(output_path,"BeforeOpt_Replicates.csv"),quote=F,row.names=F,col.names=T,sep=",")
  rm(NewReplicates_All_Results)
}

## Report ReScore
ReScoreList<-list.files(avg_results_path,pattern = paste0("Report_GR_ReScore_"))
ListFiles_ReScore<-paste0(avg_results_path,"/",ReScoreList)
if(length(ReScoreList)>=1){
  NewReScore_All_Results <- lapply(ListFiles_ReScore, fread, header = F,sep=';', stringsAsFactors = FALSE)
  NewReScore_All_Results <- rbindlist(NewReScore_All_Results)
  colnames(NewReScore_All_Results)<-c("ID_Analyte","IsotopeLabelType" , "ID_Rep",  "Similarity.Score" , "MPRA.Score" , "Library.dotp",  "Intensity.Score", "Score.MassError", "Comment")
  
  NewReScore_All_Results<-NewReScore_All_Results %>%
    mutate(ID_Analyte=as.character(ID_Analyte),
           ID_Rep=as.character(ID_Rep)) %>%
    left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
    left_join(MetaData_Replicate, by = c("ID_Rep"))
  
  Exponents<-c(9.5,4.5,2.5,0.5)
  NewReScore_All_Results<- NewReScore_All_Results %>%
    mutate(Skor=Similarity.Score^Exponents[1]*Library.dotp^Exponents[2]*Score.MassError^Exponents[3]*MPRA.Score^Exponents[4])
  
  write.table(NewReScore_All_Results,file=paste0(output_path,"AfterOpt_Replicate_Score.csv"),quote=F,row.names=F,col.names=T,sep=",")
  rm(NewReScore_All_Results)
}

## Score Annotation

ReScoreList<-list.files(avg_results_path,pattern = paste0("Report_GR_ReScore_"))
ListFiles_ReScore<-paste0(avg_results_path,"/",ReScoreList)
if(length(ReScoreList)>=1){
  ScoreAnnotations <- lapply(ListFiles_ReScore, fread, header = F,sep=';', stringsAsFactors = FALSE)
  ScoreAnnotations <- rbindlist(ScoreAnnotations)
  colnames(ScoreAnnotations)<-c("ID_Analyte","IsotopeLabelType" , "ID_Rep",  "Similarity.Score" , "MPRA.Score" , "Library.dotp",  "Intensity.Score", "Score.MassError", "Comment")
  
  Exponents<-c(9.5,4.5,2.5,0.5)
  ScoreAnnotations<-ScoreAnnotations %>%
    mutate(ID_Analyte=as.character(ID_Analyte),
           ID_Rep=as.character(ID_Rep)) %>%
    left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
    left_join(MetaData_Replicate, by = c("ID_Rep")) %>%
    select(IsotopeLabelType, ProteinName, PeptideModifiedSequence,PrecursorCharge,IsDecoy, FileName,Similarity.Score, MPRA.Score, Library.dotp,Score.MassError) %>%
    mutate(Skor=Similarity.Score^Exponents[1]*Library.dotp^Exponents[2]*Score.MassError^Exponents[3]*MPRA.Score^Exponents[4])
  
  Annotations_PrecursorResults<-MetaData_PrecursorResults %>%
    left_join(ScoreAnnotations, by = c("IsotopeLabelType", "ProteinName", "PeptideModifiedSequence", "PrecursorCharge", "IsDecoy", "FileName"))%>%
    select(PrecursorResultLocator,
           Similarity.Score,MPRA.Score,Library.dotp,Score.MassError,Skor)%>%
    mutate(Similarity.Score=ifelse(is.na(Similarity.Score),"#N/A",Similarity.Score),
           MPRA.Score=ifelse(is.na(MPRA.Score),"#N/A",MPRA.Score),
           Library.dotp=ifelse(is.na(Library.dotp),"#N/A",Library.dotp),
           Score.MassError=ifelse(is.na(Score.MassError),"#N/A",Score.MassError),
           Skor=ifelse(is.na(Skor),"#N/A",Skor)) %>%
    rename(ElementLocator=PrecursorResultLocator,
           annotation_AvG_Similarity_Score=Similarity.Score,
           annotation_AvG_MPRA_Score=MPRA.Score,
           annotation_AvG_SpectralLibSim_Score=Library.dotp,
           annotation_AvG_MassError_Score=Score.MassError,
           annotation_AvG_Score=Skor)
  
  fwrite(Annotations_PrecursorResults,file=paste0(output_path,"AnnotationsPrecursorResults.csv"), sep=",", row.names=F)
  rm(Annotations_PrecursorResults)
}
