import luigi
import hashlib
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from .luigi_avg_rtask_utils import run_r_script_for_an_analyte, run_r_script_for_all_analytes
from .parquet_file_formatting import read_hashindex_and_partition_parquetFile, convert_csv_to_parquet_by_chunks
from .parquet_file_formatting import read_by_rowgroup_hashindex_and_partition_parquetFile
from .parquet_file_formatting import parquet_partitions_to_csvs, SaltString, hash_params_file, hash_value
from .parquet_file_formatting import read_csv_by_chunks_createindices_and_partitionPQbygroup
from .parquet_file_formatting import zip_ParquetPartitions_individually
# from .parquet_file_formatting import zip_dir_keeping_folder_structure, create_indices, hashvalue_to_groupnumber_dictionary


class InputCSVFile(luigi.ExternalTask):
    # initial CSV file
    def output(self):
        # return luigi.LocalTarget(
        # "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example_only10/AvantGardeDIA_Export.csv")
        INPUT_AVG_EXPORT_REPORT = os.getenv('INPUT_AVG_EXPORT_REPORT')
        return luigi.LocalTarget(INPUT_AVG_EXPORT_REPORT)


class ParamsFile(luigi.ExternalTask):
    # Parameters file for the avant-garde R script
    def output(self):
        # return luigi.LocalTarget(
        #     "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example_only10/AvG_Params.R")
        INPUT_AVG_PARAMS = os.getenv('INPUT_AVG_PARAMS')
        return luigi.LocalTarget(INPUT_AVG_PARAMS)

class OutputDirectory(luigi.ExternalTask):
    # Parameters file for the avant-garde R script
    def output(self):
        # return luigi.LocalTarget(
        #     "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example_only10/AvG_Params.R")
        INPUT_AVG_PARAMS = os.getenv('OUTPUT_DIRECTORY')
        return luigi.LocalTarget(INPUT_AVG_PARAMS)


class ConvertCSVToZippedParquetDataset(luigi.Task):
    # Converts CSV to parquet file
    def requires(self):
        return {'inputcsv': self.clone(InputCSVFile),
                'params_file': self.clone(ParamsFile),
                'output_directory': self.clone(OutputDirectory)}

    def output(self):
        # salted graph using the parameters used for the R script and the name of the initial csv file
        params_tag = hash_params_file(self.input()['params_file'].path)
        # csv_file_tag = SaltString.get_hash_of_file(self.input()['inputcsv'].path)
        # hex_tag = hash_value(csv_file_tag + params_tag)

        hex_tag = hash_value(params_tag)

        return luigi.LocalTarget("data/AvantGardeDIA_Export_%s.parquet" % hex_tag)

    def run(self):
        # convert_csv_to_parquet_by_chunks(input_csv_path=self.input()['inputcsv'].path,
        #                                  parquet_file_path=self.output().path,
        #                                  chunksize=2000)

        output_directory = self.input()['output_directory'].path

        pq_dataset_path = os.path.join(output_directory, "pq_dataset")
        os.mkdir(pq_dataset_path)

        indices_glossary_path = os.path.join(output_directory, "indices_glossary")
        os.mkdir(indices_glossary_path)

        zip_files_path = os.path.join(output_directory, "zip_files")
        os.mkdir(zip_files_path)

        read_csv_by_chunks_createindices_and_partitionPQbygroup(
            input_csv_path=self.input()['inputcsv'].path,
            parquet_dataset_output_path=pq_dataset_path,
            indices_csv_output_path=indices_glossary_path,
            chunksize=1000,
            n_parts=10)

        zip_ParquetPartitions_individually(
            parquet_dataset_path=pq_dataset_path,
            zip_outputs=zip_files_path)


# class UnzipPartitionsConvertToCSV(luigi.Task):
#     # Unzip pq partitions and converts each parquet partition to a csv file
#
#     parquet_dataset_dirpath = ReadHashIndexAndPartitionParquetFile.root_path
#     csv_ds_root_path = ReadHashIndexAndPartitionParquetFile.csv_ds_root_path
#
#     def requires(self):
#         return ReadHashIndexAndPartitionParquetFile()
#     def output(self):
#         # hex_tag = SaltString.get_hash_of_file(self.input().path)
#         hex_tag = "hi"
#         return luigi.LocalTarget(self.csv_ds_root_path+'ID_Analyte_glossary_2'+"_%s.csv" % hex_tag)
#     def run(self):
#
#
#         parquet_partitions_to_csvs(id_analyte_path=self.input().path,
#                                    parquet_dataset_dirpath=self.parquet_dataset_dirpath,
#                                    output_dirpath=self.csv_ds_root_path)
#         df = pd.read_csv(self.input().path)
#         df.to_csv(self.output().path, index=False)
