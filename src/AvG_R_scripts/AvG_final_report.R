.libPaths(c( "C:/Users/svaca/Documents/R/win-library/3.5" , "C:/Program Files/R/R-3.5.2/library"))

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
output_path <- as.character(args[6])
success_file <- as.character(args[7])


# params_file <- "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example_only10/AvG_Params.R"
# avg_results_path<- "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/pset_3_cookiecutter/2019sp-final-project-SebVaca/data/avg_results/"
# MetaData_Analytes<- fread("C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/pset_3_cookiecutter/2019sp-final-project-SebVaca/data/avg_results/ID_Analyte_glossary_3_2a535757.csv",
#                           stringsAsFactors = F) %>% change_names()
# Transition_Locator<- fread("C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/pset_3_cookiecutter/2019sp-final-project-SebVaca/data/csv_ds/ID_transition_locator.csv",
#                           stringsAsFactors = F) %>% change_names()
# MetaData_Replicate<- fread("C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/pset_3_cookiecutter/2019sp-final-project-SebVaca/data/csv_ds/ID_Rep.csv",
#                            stringsAsFactors = F) %>% change_names()
# output_path<- "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/pset_3_cookiecutter/2019sp-final-project-SebVaca/data/final_result/"

MetaData_Analytes<- fread(MetaData_Analytes_path, stringsAsFactors = F) %>% change_names()
Transition_Locator<- fread(Transition_Locator, stringsAsFactors = F) %>% change_names()
MetaData_Replicate<- fread(MetaData_Replicate, stringsAsFactors = F) %>% change_names()

source(params_file)
## Peak_BOundaries
PBlist<-list.files(avg_results_path, pattern = paste0("Report_GR_PeakBoundaries_"))
ListFiles_PeakBoundaries<-paste0(avg_results_path,"/",PBlist)

if(length(PBlist)>=1){
  l <- lapply(ListFiles_PeakBoundaries, fread, header = F,sep=';', stringsAsFactors = FALSE)
  NewPeakBoundaries_All_Results <- rbindlist( l )
  
  colnames(NewPeakBoundaries_All_Results)<-c("MinStartTime","MaxEndTime", "ID_Rep", "ID_Analyte")
  
  NewPeakBoundaries_All_Results<-NewPeakBoundaries_All_Results %>% 
    mutate(MinStartTime=ifelse(is.na(MinStartTime),"#N/A",MinStartTime),
           MaxEndTime=ifelse(is.na(MaxEndTime),"#N/A",MaxEndTime)) %>% 
    mutate(ID_Analyte=as.character(ID_Analyte),
           ID_Rep=as.character(ID_Rep)) %>%
    left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
    left_join(MetaData_Replicate, by = c("ID_Rep"))
  
  
  Formatted<-NewPeakBoundaries_All_Results %>%
    select(FileName,PeptideModifiedSequence,MinStartTime,MaxEndTime,PrecursorCharge,IsDecoy) %>%
    rename(PrecursorIsDecoy=IsDecoy) %>%
    distinct()
  
  write.csv(Formatted,file=paste0(output_path,"Peak_Boundaries_results.csv"),quote=F,row.names=F)
}

## Report Transitions
Translist<-list.files(avg_results_path,pattern = paste0("Report_GR_Transitions_"))
ListFiles_Transitions<-paste0(avg_results_path,"/",Translist)
if(length(Translist)>=1){
  l_trans <- lapply(ListFiles_Transitions, fread, header = F,sep=';', stringsAsFactors = FALSE)
  NewTransitions_All_Results <- rbindlist( l_trans )
  colnames(NewTransitions_All_Results)<-c("ID_FragmentIon_charge","ID_Analyte")
  
  NewTransitions_All_Results<-NewTransitions_All_Results %>% mutate(Quantitative= "TRUE")
  
  Trans_results<-Transition_Locator %>%
    full_join(NewTransitions_All_Results, by = c("ID_FragmentIon_charge", "ID_Analyte")) %>%
    mutate(Quantitative= ifelse(is.na(Quantitative),FALSE,TRUE)) %>%
    rename(ElementLocator = TransitionLocator) %>%
    select(ElementLocator, Quantitative)
  
  write.table(Trans_results,file=paste0(output_path,"Transition_results.csv"),quote=F,row.names=F,col.names=T,sep=",")
}  


write.table(
  fread(MetaData_Analytes_path, stringsAsFactors = F), file = success_file, quote=F,row.names=F,col.names=T,sep=",")
