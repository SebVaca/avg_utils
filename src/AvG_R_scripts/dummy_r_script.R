args <- commandArgs(TRUE)
analyte_data <- as.character(args[1])
params_file <- as.character(args[2])
analyte_hash_id <- as.character(args[3])
output_dir <- as.character(args[4])


source(params_file)
results<- paste0("the path is : ", as.character(analyte_data), "\n",
                "params file is : ", as.character(params_file),"\n",
                "hash id file is : ", as.character(analyte_hash_id),
                "sourced parameter (alpha): ", alpha, "\n", 
                "output_dir: ", output_dir)

data<-read.csv(analyte_data)
write.table(results,file="C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example/it_works.txt",quote=F,row.names=F,col.names=F,sep=";")
write.table(data,file="C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example/it_works_data.txt",quote=F,row.names=F,col.names=F,sep=";")
