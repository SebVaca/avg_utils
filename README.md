# Integrating an R script as a task in a Luigi workflow

I work in the field of Proteomics and I use a method called mass-spectrometry to quantify peptides to understand how drugs modulate interesting protein modifications in cancerous cells. The signals we measure are called chromatograms, they correspond to the intensity of each peptide as a function of time. I have developed an R package, called Avant-garde (AvG), that curates the data automatically to remove noise and interfering signals. 

![AvG](http://drive.google.com/uc?export=view&id=1QOqZKxeFiQYlkPiX-07a4BMpROpuSmyh)

For my project, I would like to improve my analysis workflow by using parquet files, and a Luigi workflow. My workflow starts from large CSV files (>50Gb) which contain the data for all peptides (8000-14000) measured in several samples (typically around 100 samples). Each row in this file contains the information for a given peptide in a given sample. A peptide is defined by its sequence, its charge and the name of the protein it belongs to. Since this information is written in every row the data is very redundant. In order to query the data faster and in parallel it is converted into an SQLite database. The conversion is done by reading the CSV file in chunks, doing some data wrangling on each chunk and then adding the information into the SQLite file. The SQLite is then indexed by a column containing a unique integer created by concatenating the information of the sequence, charge and protein name of each peptide. The final SQLite database is around the same size as the input CSV file (>50Gb).

# Results
## 1) Use a parquet file instead of an SQLite database.

- The initial CSV file was read in chunks and written to a parquet file.
- A unique index for each analyte was created by concatenating several columns together to ensure the uniqueness of each analyte (peptide sequence, peptide charge, and protein name) and by calculating a hashed value for it.
- This hashed id for each analyte was used to partition the parquet file

The file CSV to Parquet file was really fast and the file size was 2 to 3 times smaller that the original CSV or the SQLite database. 

## 2) Use Luigi to manage and automate the workflow integrating an R script as a luigi task.
- I have used the `subprocess` module to call and pass arguments to the `Rscript.exe` executable.
- It is necessary to provide absolute paths to the `subprocess` module. The paths to the `Rscript.exe` and the App were added as an environment variables in the `.env` file.
- In order to be sure that the R script has run I have made sure that a `_SUCCESS` file is written at the final step of the R script. It this is not done if the R script fails, luigi will not output an error and will continue running. My `_SUCCESS` file was the `ID_analyte_glossary` that contains the hashed IDs of all analytes.

The workflow has 2 external tasks for the initial parameters:
- `InputCSVFile`: provides the path to the original CSV file
- `ParamsFile`: provides the path to the parameters file necessary to run the `avant-garde` R script

and  5 luigi tasks for the main data analysis:
- `ConvertCSVToParquet`: Converts CSV to parquet file by reading the CSV file in chunks
- `ReadHashIndexAndPartitionParquetFile`: Reads parquet file, creates index for each analyte and uses that index to partition the parquet file
- `TransformParquetPartitionsToCSV`: Transforms each parquet partition to a csv file.
- `RTask_AvantGarde`: Runs the `avant-garde`R script on the individual csv files containing the data of each analyte.
- `RTask_Report`: Summarize all the individual csv containing the results that were created by the `avant-garde` R script and creates a report.

The workflow works and we are able to curate mass spectrometry data automatically only providing the two initial files.
# Overview of the scripts
- `final_project/parquet_file_formatting.py`: functions to convert the csv to a parquet file, index and partition it
- `final_project/luigi_avg_rtask_utils.py`: tools to run the `avant-garde` R script in Luigi
- `final_project/AvG_luigi_pipeline.py`: Main Luigi workflow script
- `AvG_R_scripts/AvG_from_partitionedParquet.R`: R script to run `avant-garde` on a partitioned parquet file
- `AvG_R_scripts/AvG_final_report.R`: R script to summarise the individual files and create a the final report

# Run the pipeline locally
`pipenv run luigi --module final_project.AvG_luigi_pipeline RTask_Report --local-scheduler`

# References

- http://opiateforthemass.es/articles/r-in-big-data-pipeline/
- https://arrow.apache.org/docs/python/parquet.html
- https://www.r-bloggers.com/passing-arguments-to-an-r-script-from-command-lines/
- https://stackoverflow.com/questions/26124417/how-to-convert-a-csv-file-to-parquet