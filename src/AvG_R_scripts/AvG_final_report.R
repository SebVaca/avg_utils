# .libPaths(c( "C:/Users/svaca/Documents/R/win-library/3.5" , "C:/Program Files/R/R-3.5.2/library"))
# .libPaths(c( "C:/Users/svaca/Documents/R/win-library/4.0", "C:/Program Files/R/R-4.0.3/library", "I:/R/win-library/4.0"))
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
MetaData_PrecursorResults<- fread(MetaData_PrecursorResults, stringsAsFactors = F) %>% change_names()##new

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

###########NEW
## Report Replicates
  RepList<-list.files(avg_results_path,pattern = paste0("Report_GR_Replicate_"))
  ListFiles_Replicates<-paste0(avg_results_path,"/",RepList)
  if(length(RepList)>=1){
    l_rep <- lapply(ListFiles_Replicates, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NewReplicates_All_Results <- rbindlist( l_rep )
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
  }

  ## Report ReScore
  ReScoreList<-list.files(avg_results_path,pattern = paste0("Report_GR_ReScore_"))
  ListFiles_ReScore<-paste0(avg_results_path,"/",ReScoreList)
  if(length(ReScoreList)>=1){
    l_reScore <- lapply(ListFiles_ReScore, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NewReScore_All_Results <- rbindlist(l_reScore)
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
  }

  ## Score Annotation

  ReScoreList<-list.files(avg_results_path,pattern = paste0("Report_GR_ReScore_"))
  ListFiles_ReScore<-paste0(avg_results_path,"/",ReScoreList)
  if(length(ReScoreList)>=1){
    l_reScore <- lapply(ListFiles_ReScore, fread, header = F,sep=';', stringsAsFactors = FALSE)
    ScoreAnnotations <- rbindlist(l_reScore)
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
  }

######################################

Read_AndFormatResults<-function(){
  Folder_3="ResultsOptimization"
  dir.create(file.path(getwd(),Folder_3),showWarnings = F)
  output_path=file.path(getwd(),Folder_3)
  TempFiles.Location<-avg_results_path

  ##
  RefinementTAG<-if(RefinementWorkflow=="GlobalRefinement") {"GR"} else{
    if(RefinementWorkflow=="TransitionRefinement") {"TR"} else{
      if(RefinementWorkflow=="PeakBoundariesRefinement") {"PB"} else{
        if(RefinementWorkflow=="OnlyScoring") {"NO"}
      }}}

  ## Column Names
  ColumnNames<-read.csv(file = paste0(avg_results_path,"/Report_",RefinementTAG,"_ColNames",Name_Tag,".csv"),header = F,sep=' ', stringsAsFactors = FALSE)

  ## MetaData
  db <- dbConnect(SQLite(), dbname=paste0("DB_",Name_Tag,".sqlite"))
  MetaData_Analytes<-dbReadTable(db, "MetaData_Analyte") %>% mutate(ID_Analyte=as.character(ID_Analyte))
  MetaData_Replicate<-dbReadTable(db, "MetaData_Replicate") %>% mutate(ID_Rep=as.character(ID_Rep))
  MetaData_Transitions<-dbReadTable(db, "MetaData_Transitions") %>% mutate(ID_Analyte=as.character(ID_Analyte))
  MetaData_PrecursorResults<-dbReadTable(db, "MetaData_PrecursorResults")

  dbDisconnect(db)

  ## Peak_BOundaries
  PBlist<-list.files(avg_results_path,pattern = paste0("Report_",RefinementTAG,"_PeakBoundaries_"))
  ListFiles_PeakBoundaries<-paste0(avg_results_path,"/",PBlist)
  if(length(PBlist)>=1){
    l <- lapply(ListFiles_PeakBoundaries, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NewPeakBoundaries_All_Results <- rbindlist( l )

    colnames(NewPeakBoundaries_All_Results)<-strsplit(ColumnNames[3,],split = ";",fixed = T)[[1]]

    NewPeakBoundaries_All_Results<-NewPeakBoundaries_All_Results %>%
      mutate(left=ifelse(is.na(left),"#N/A",left),
             right=ifelse(is.na(right),"#N/A",right)) %>%
      mutate(ID_Analyte=as.character(ID_Analyte),
             ID_Rep=as.character(ID_Rep))%>%
      left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
      left_join(MetaData_Replicate, by = c("ID_Rep"))

    write.table(NewPeakBoundaries_All_Results,file=paste0(output_path,"/",RefinementTAG,"_NewPeakBoundaries_",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")

    Formatted<-NewPeakBoundaries_All_Results %>%
      select(FileName,PeptideModifiedSequence,left,right,PrecursorCharge,IsDecoy,IsotopeLabelType) %>%
      rename(PrecursorIsDecoy=IsDecoy) %>%
      rename(MinStartTime=left,MaxEndTime=right) %>%
      distinct()

    if(UseHeavyPeakBoundariesForLight==TRUE) {Formatted<-Formatted %>% filter(IsotopeLabelType=="heavy")} ##Only for P100

    write.csv(Formatted,file=paste0(output_path,"/",RefinementTAG,"_NewPeakBoundaries_",Name_Tag,"_Formated.csv"),quote=F,row.names=F)
  }

  ## Report Transitions
  Translist<-list.files(avg_results_path,pattern = paste0("Report_",RefinementTAG,"_Transitions_"))
  ListFiles_Transitions<-paste0(avg_results_path,"/",Translist)
  if(length(Translist)>=1){

    l_trans <- lapply(ListFiles_Transitions, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NewTransitions_All_Results <- rbindlist( l_trans )
    colnames(NewTransitions_All_Results)<-strsplit(ColumnNames[1,],split = ";",fixed = T)[[1]]

    NewTransitions_All_Results<-NewTransitions_All_Results %>%
      mutate(ID_Analyte=as.character(ID_Analyte),
             ID_Rep=as.character(ID_Rep)) %>%
      left_join(MetaData_Transitions, by = c("ID_FragmentIon_charge","ID_Analyte", "IsotopeLabelType")) %>%
      left_join(MetaData_Replicate, by = c("ID_Rep"))

    write.table(NewTransitions_All_Results,file=paste0(output_path,"/",RefinementTAG,"_NewTransitions_",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")


    NewTransitions_All_Results_OnlyTransitionsInfo<-data.frame(NewTransitions_All_Results %>% select(-Similarity.Score,-ID_Rep) %>%
                                                                 distinct())
    write.table(NewTransitions_All_Results_OnlyTransitionsInfo,file=paste0(output_path,"/",RefinementTAG,"_NewTransitions_","OnlyTransitionInfo",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")
  }


  ## Report Replicates
  RepList<-list.files(avg_results_path,pattern = paste0("Report_",RefinementTAG,"_Replicate_"))
  ListFiles_Replicates<-paste0(avg_results_path,"/",RepList)
  if(length(RepList)>=1){
    l_rep <- lapply(ListFiles_Replicates, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NewReplicates_All_Results <- rbindlist( l_rep )
    colnames(NewReplicates_All_Results)<-strsplit(ColumnNames[2,],split = ";",fixed = T)[[1]]

    NewReplicates_All_Results<-NewReplicates_All_Results %>%
      mutate(ID_Analyte=as.character(ID_Analyte),
             ID_Rep=as.character(ID_Rep)) %>%
      left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
      left_join(MetaData_Replicate, by = c("ID_Rep"))

    Exponents<-c(9.5,4.5,2.5,0.5)
    NewReplicates_All_Results<-NewReplicates_All_Results %>%
      mutate(Skor=Similarity.Score^Exponents[1]*Library.dotp^Exponents[2]*Score.MassError^Exponents[3]*MPRA.Score^Exponents[4])

    write.table(NewReplicates_All_Results,file=paste0(output_path,"/", RefinementTAG,"_BeforeOpt_Replicates_",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")
  }

  ## Report ReScore
  ReScoreList<-list.files(avg_results_path,pattern = paste0("Report_",RefinementTAG,"_ReScore_"))
  ListFiles_ReScore<-paste0(avg_results_path,"/",ReScoreList)
  if(length(ReScoreList)>=1){
    l_reScore <- lapply(ListFiles_ReScore, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NewReScore_All_Results <- rbindlist(l_reScore)
    colnames(NewReScore_All_Results)<-strsplit(ColumnNames[2,],split = ";",fixed = T)[[1]]

    NewReScore_All_Results<-NewReScore_All_Results %>%
      mutate(ID_Analyte=as.character(ID_Analyte),
             ID_Rep=as.character(ID_Rep)) %>%
      left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
      left_join(MetaData_Replicate, by = c("ID_Rep"))

    Exponents<-c(9.5,4.5,2.5,0.5)
    NewReScore_All_Results<- NewReScore_All_Results %>%
      mutate(Skor=Similarity.Score^Exponents[1]*Library.dotp^Exponents[2]*Score.MassError^Exponents[3]*MPRA.Score^Exponents[4])

    write.table(NewReScore_All_Results,file=paste0(output_path,"/",RefinementTAG,"_AfterOpt_Replicate_Score_",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")
  }

  ## Score Annotation

  ReScoreList<-list.files(avg_results_path,pattern = paste0("Report_",RefinementTAG,"_ReScore_"))
  ListFiles_ReScore<-paste0(avg_results_path,"/",ReScoreList)
  if(length(ReScoreList)>=1){
    l_reScore <- lapply(ListFiles_ReScore, fread, header = F,sep=';', stringsAsFactors = FALSE)
    ScoreAnnotations <- rbindlist(l_reScore)
    colnames(ScoreAnnotations)<-strsplit(ColumnNames[2,],split = ";",fixed = T)[[1]]

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

    fwrite(Annotations_PrecursorResults,file=paste0(output_path,"/",RefinementTAG,"_AnnotationsPrecursorResults_",Name_Tag,".csv"), sep=",", row.names=F)
  }

  ## No Result
  if (RefinementTAG %in% c("TR","PB","GR")) {
    Extract_Indices_FromFileNames<-function(ListFiles){
      ListFiles<-gsub(ListFiles,pattern = Name_Tag,replacement = "TAG")

      ListFiles<-data.frame(NameFiles=ListFiles) %>%
        mutate(Index=gsub(
          substr(NameFiles,start = str_locate(pattern = "TAG_",NameFiles)[2]+4,stop = nchar(as.character(NameFiles))),
          pattern=".csv",replacement = "")) %>% arrange(as.numeric(Index))

      #HighestIndex=max(as.numeric(ListFiles$Index),na.rm = T)
      #setdiff(1:HighestIndex,ListFiles$Index)
      #length(setdiff(1:HighestIndex,ListFiles$Index))

      return(ListIndeces<-ListFiles$Index)}

    #NoResult<-unique(setdiff(1:max(as.numeric(Extract_Indices_FromFileNames(ListFiles_ReScore))),Extract_Indices_FromFileNames(ListFiles_ReScore)))
    NoResult<-unique(setdiff(1:max(as.numeric(Extract_Indices_FromFileNames(ReScoreList))),Extract_Indices_FromFileNames(ReScoreList)))
    NoResult<-MetaData_Analytes %>% filter(ID_Analyte %in% NoResult) %>% select(PeptideModifiedSequence, IsDecoy,ID_Analyte) %>% distinct()
    write.table(NoResult,file=paste0(output_path,"/",RefinementTAG,"_NoResult_",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")
  }


  ## Import Annotations into Skyline
  if (RefinementTAG %in% c("TR","GR")) {
    NewTransitions_All_Results_OnlyTransitionsInfo_2<-NewTransitions_All_Results_OnlyTransitionsInfo %>%
      select(TransitionLocator) %>% mutate(Quantitative="TRUE") %>%
      #mutate(PrecursorMz=round(PrecursorMz,4),
      #       ProductMz=round(ProductMz,4)) %>%
      distinct()

    A<-MetaData_Transitions %>%
      select(TransitionLocator) %>%
      distinct() %>%
      #mutate(PrecursorMz=round(PrecursorMz,4),ProductMz=round(ProductMz,4)) %>%
      left_join(NewTransitions_All_Results_OnlyTransitionsInfo_2, by = c("TransitionLocator")) %>%
      mutate(Quantitative=ifelse(is.na(Quantitative),"FALSE",Quantitative)) %>%
      rename(ElementLocator=TransitionLocator)

    fwrite(A,paste0(output_path,"/",RefinementTAG,"_Transitions_Annotations_",Name_Tag,".csv"), sep=",", row.names = F)

  }

  ############ Cut_off Peak Boundaries

  determine_FDR_AvG <- function(NewReScore_All_Results){

    NewReScore_All_Results<-NewReScore_All_Results %>%
      mutate(IsDecoy=gsub(IsDecoy,pattern = "False",replacement = 0)) %>%
      mutate(IsDecoy=gsub(IsDecoy,pattern = "True",replacement = 1))

    Q<-NewReScore_All_Results

    U<-Q

    U<-U %>% arrange(-Skor)

    U<-U %>% arrange(IsDecoy) %>% arrange(-Skor)

    U2<-data.frame(U$IsDecoy,count=ave(U$IsDecoy==U$IsDecoy,U$IsDecoy, FUN=cumsum))

    U3<-cbind(U,U2)
    U3$Num<-1:dim(U3)[1]
    U4<-U3 %>% mutate(FDR=ifelse(IsDecoy==1,count/Num*100,NA))

    FDR_1Percent<-as.numeric(U4 %>%  filter(FDR>=1) %>% filter(row_number()==1) %>% select(Skor))

    U5<-U4 %>% filter(IsDecoy==1)%>%select(Skor,FDR)


    b1=ggplot(U5,aes(x= Skor,y=FDR))+
      geom_line(size=1)+
      geom_vline(xintercept = FDR_1Percent,size=1,linetype=2,color="#FF0000")+
      geom_hline(yintercept = 1,linetype=2,size=1,color="black")+
      theme_bw()+scale_x_continuous(limits = c(0,1),breaks = seq(0,1,0.1))+
      theme(legend.position = "none",axis.text.x=element_text(angle = 45,vjust = 0.05))+
      annotate("text", x = 0.175,y=15, label = "FDR=1%",color = "#FF0000",angle = 90)+
      labs(y="FDR (%)",x="AvG score")


    Z=ggplot(Q,aes(x= Skor,fill=paste0(IsDecoy)))+
      geom_histogram(binwidth = 0.05,alpha=0.5,position="identity",color="black")+
      theme_bw()+
      geom_vline(xintercept = FDR_1Percent,linetype=2,size=1,color="#FF0000")+
      scale_x_continuous(breaks = seq(0,1,0.1))+
      scale_fill_manual(values = c("#5BBCD6", "#FF0000"))+
      theme(legend.position = "none", axis.text.x=element_text(angle = 45,vjust = 0.05))+
      annotate("text", size=3, x = 0.175,y=5000, label = "FDR=1%",color = "#FF0000",angle = 90)+
      theme_classic()+theme(legend.position = "none", axis.text.x=element_text(angle = 45,vjust = 0.05))+
      labs(y="Count",x="AvG score")


    FDR_estimation <- grid.arrange(
      Z+labs(y="Number of peptides")+theme_bw()+
        theme(#axis.text.y = element_blank(),
          #axis.ticks.y = element_blank(),
          legend.position = "none",
          axis.text.x=element_text(angle = 45,vjust = 0.05)),

      b1+theme_bw()+
        theme(legend.position = "none", axis.text.x=element_text(angle = 45,vjust = 0.05)),
      ncol=1)

    ggsave(FDR_estimation,
           filename = paste0(output_path,"/",RefinementTAG,"_FDR_plot_",Name_Tag,".pdf"),
           width = 6,height =9)

    return(FDR_1Percent=FDR_1Percent)}

  Cut_off_value = ifelse(length(unique(MetaData_Analytes$IsDecoy))>1,
                       determine_FDR_AvG(NewReScore_All_Results),
                       ifelse(NonZeroBaselineChromatogram==TRUE,0.61,0.1))

  cut_off_tag = gsub(paste0(ifelse(length(unique(MetaData_Analytes$IsDecoy))>1,
                       "FDR_Below1Percent_cutoffAvGScore_",
                       "Fixed_cutoffAvGScore_"), round(Cut_off_value,3)),pattern = "\\.",replacement = "pt")

  if (RefinementTAG %in% c("PB","GR")) {
    PeakBoundaries_Final<-NewReScore_All_Results%>%
      filter(as.numeric(Library.dotp)>=0.7,
             as.numeric(Similarity.Score)>=0.85,
             as.numeric(Score.MassError)>=0.7,
             as.numeric(MPRA.Score)>=0.9,
             as.numeric(Skor)>=Cut_off_value)

    Num_Of_ValidReplicate_per_Peptide<-PeakBoundaries_Final %>%
      group_by(ID_Analyte,IsotopeLabelType,Comment,ProteinName,PeptideModifiedSequence, PrecursorCharge,IsDecoy) %>%
      summarise(n=n()) %>% ungroup() %>% rename(Num_Replicates=n)

    PeakBoundaries_Final<-PeakBoundaries_Final %>% left_join(Num_Of_ValidReplicate_per_Peptide,by = c("ID_Analyte", "IsotopeLabelType", "Comment", "ProteinName", "PeptideModifiedSequence", "PrecursorCharge", "IsDecoy")) %>%
      select(ID_Rep,ID_Analyte,IsotopeLabelType,FileName,ProteinName,PeptideModifiedSequence,PrecursorCharge,IsDecoy,Num_Replicates) %>%
      distinct() %>%
      mutate(Keep="Keep")


    W<-NewPeakBoundaries_All_Results %>% left_join(PeakBoundaries_Final, by = c("ID_Rep", "ID_Analyte", "IsotopeLabelType", "FileName", "ProteinName", "PeptideModifiedSequence","PrecursorCharge", "IsDecoy")) %>%
      mutate(left2=ifelse(Keep=="Keep",left,NA),right2=ifelse(Keep=="Keep",right,NA)) %>%
      mutate(left=left2,right=right2) %>% select(-left2,-right2) %>%
      mutate(left=ifelse(is.na(left),"#N/A",left),
             right=ifelse(is.na(right),"#N/A",right))

    Formatted_Filtered<-W %>%
      select(FileName,PeptideModifiedSequence,left,right,PrecursorCharge,IsDecoy,IsotopeLabelType) %>%
      rename(PrecursorIsDecoy=IsDecoy) %>%
      rename(MinStartTime=left,MaxEndTime=right) %>%
      distinct()

    if(UseHeavyPeakBoundariesForLight==TRUE) {Formatted_Filtered<-Formatted_Filtered %>% filter(IsotopeLabelType=="heavy")} ##Only for P100

    write.csv(Formatted_Filtered,file=paste0(output_path,"/",RefinementTAG,"_NewPeakBoundaries_",Name_Tag,"_Formatted_Filtered_",cut_off_tag,".csv"),quote=F,row.names=F)}

  ## Only Scoring / No optimization
  NoOptimizationList<-list.files(avg_results_path,pattern = paste0("Report_",RefinementTAG,"_ScoreNonOptimized_"))
  ListFiles_NoOpt<-paste0(avg_results_path,"/",NoOptimizationList)
  if(length(NoOptimizationList)>=1){
    l_NoOpt <- lapply(ListFiles_NoOpt, fread, header = F,sep=';', stringsAsFactors = FALSE)
    NoOpt_All_Results <- rbindlist(l_NoOpt)
    colnames(NoOpt_All_Results)<-strsplit(ColumnNames[2,],split = ";",fixed = T)[[1]]

    NoOpt_All_Results<-NoOpt_All_Results %>%
      mutate(ID_Analyte=as.character(ID_Analyte),
             ID_Rep=as.character(ID_Rep)) %>%
      left_join(MetaData_Analytes, by = c("ID_Analyte")) %>%
      left_join(MetaData_Replicate, by = c("ID_Rep"))

    Exponents<-c(9.5,4.5,2.5,0.5)
    NoOpt_All_Results<- NoOpt_All_Results %>%
      mutate(Skor=Similarity.Score^Exponents[1]*Library.dotp^Exponents[2]*Score.MassError^Exponents[3]*MPRA.Score^Exponents[4])

    write.table(NoOpt_All_Results,file=paste0(output_path,"/",RefinementTAG,"_NoOptimization_Score_",Name_Tag,".csv"),quote=F,row.names=F,col.names=T,sep=",")
  }

  print("Format Results: Done!")

}
