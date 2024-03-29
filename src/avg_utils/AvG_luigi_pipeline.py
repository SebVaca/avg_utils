import luigi
import hashlib
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from .luigi_avg_rtask_utils import run_r_script_for_all_analytes_luigi
from .parquet_file_formatting import convert_csv_to_parquet_by_chunks
from .parquet_file_formatting import read_by_rowgroup_hashindex_and_partition_parquetFile
from .parquet_file_formatting import parquet_partitions_to_csvs, SaltString, hash_params_file, hash_value


class InputCSVFile(luigi.ExternalTask):
    # initial CSV file
    def output(self):
        INPUT_AVG_EXPORT_REPORT = os.getenv('INPUT_AVG_EXPORT_REPORT')
        return luigi.LocalTarget(INPUT_AVG_EXPORT_REPORT)


class ParamsFile(luigi.ExternalTask):
    # Parameters file for the avant-garde R script
    def output(self):
        # return luigi.LocalTarget(
        #     "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example_only10/AvG_Params.R")
        INPUT_AVG_PARAMS = os.getenv('INPUT_AVG_PARAMS')
        return luigi.LocalTarget(INPUT_AVG_PARAMS)


class ConvertCSVToParquet(luigi.Task):
    # Converts CSV to parquet file
    def requires(self):
        return {'inputcsv': self.clone(InputCSVFile),
                'params_file': self.clone(ParamsFile)}

    def output(self):
        # salted graph using the parameters used for the R script
        params_tag = hash_params_file(self.input()['params_file'].path)
        hex_tag = hash_value(params_tag)

        return luigi.LocalTarget("data/AvantGardeDIA_Export_%s.parquet" % hex_tag)

    def run(self):
        convert_csv_to_parquet_by_chunks(input_csv_path=self.input()['inputcsv'].path,
                                         parquet_file_path=self.output().path,
                                         chunksize=2000)


class ReadHashIndexAndPartitionParquetFile(luigi.Task):
    # Reads parquet file, creates index for each analyte and creates partitions for the parquet file
    root_path = luigi.Parameter(default='data/pq_ds/', is_global=True)
    csv_ds_root_path = luigi.Parameter(default='data/csv_ds/', is_global=True)

    def requires(self):
        return ConvertCSVToParquet()
    def output(self):
        # hex_tag = SaltString.get_hash_of_file(self.input().path)
        hex_tag ="hi"
        return luigi.LocalTarget(self.csv_ds_root_path+'ID_Analyte_glossary'+"_%s.csv" % hex_tag)
    def run(self):

        read_by_rowgroup_hashindex_and_partition_parquetFile(input_path=self.input().path,
                                                             rootpath=self.root_path,
                                                             csv_ds_root_path=self.csv_ds_root_path,
                                                             id_analyte_path=self.output().path)


class TransformParquetPartitionsToCSV(luigi.Task):
    # transforms each parquet partition to a csv file

    parquet_dataset_dirpath = ReadHashIndexAndPartitionParquetFile.root_path
    csv_ds_root_path = ReadHashIndexAndPartitionParquetFile.csv_ds_root_path

    def requires(self):
        return ReadHashIndexAndPartitionParquetFile()
    def output(self):
        hex_tag = "hi"
        return luigi.LocalTarget(self.csv_ds_root_path+'ID_Analyte_glossary_2'+"_%s.csv" % hex_tag)
    def run(self):
        parquet_partitions_to_csvs(id_analyte_path=self.input().path,
                                   parquet_dataset_dirpath=self.parquet_dataset_dirpath,
                                   output_dirpath=self.csv_ds_root_path)
        df = pd.read_csv(self.input().path)
        df.to_csv(self.output().path, index=False)


class RTask_AvantGarde(luigi.Task):
    # Runs AvG_from_partitionedParquet R script on the individual csv files containing the data of each analyte.

    output_dir = luigi.Parameter(default='data/avg_results/', is_global=True)

    csv_ds_root_path = ReadHashIndexAndPartitionParquetFile.csv_ds_root_path
    R_SCRIPT_PATH = os.getenv('R_SCRIPT_PATH')
    local_path = os.getenv('local_path')

    def requires(self):
        return {'params_file': self.clone(ParamsFile),
                'ID_analyte_glossary': self.clone(TransformParquetPartitionsToCSV)}

    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input()['ID_analyte_glossary'].path)
        return luigi.LocalTarget(self.output_dir + 'ID_Analyte_glossary_3' + "_%s.csv" % hex_tag)

    def run(self):
        input_path = self.input()['ID_analyte_glossary'].path
        params_file_path = self.input()['params_file'].path

        run_r_script_for_all_analytes_luigi(id_analyte_path=input_path,
                                      csv_ds_root_path=self.csv_ds_root_path,
                                      params_file_path=params_file_path,
                                      output_dir=self.output_dir)

        df = pd.read_csv(input_path)
        df.to_csv(self.output().path, index=False)


class RTask_Report(luigi.Task):
    # Runs the AvG_final_report to create a summary of all the individual csv containing the results that were created
    # by the AvG_from_partitionedParquet R script.

    final_results_dir = luigi.Parameter(default='data/final_result/', is_global=True)
    avg_results_path = RTask_AvantGarde.output_dir
    csv_ds_root_path = ReadHashIndexAndPartitionParquetFile.csv_ds_root_path
    R_SCRIPT_PATH = os.getenv('R_SCRIPT_PATH')
    local_path = os.getenv('local_path')

    def requires(self):
        return {'params_file': self.clone(ParamsFile),
                'ID_analyte_glossary': self.clone(RTask_AvantGarde)}

    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input()['ID_analyte_glossary'].path)
        return luigi.LocalTarget(self.final_results_dir + 'ID_Analyte_glossary_4' + "_%s.csv" % hex_tag)

    def run(self):
        input_path = self.input()['ID_analyte_glossary'].path
        params_file_path = self.input()['params_file'].path
        print(input_path)
        subprocess_call_for_r_script = str(
            self.R_SCRIPT_PATH +
            ' "' + self.local_path + 'src/AvG_R_scripts/AvG_final_report.R' + '" ' +
            ' "' + str(params_file_path) + '" ' +
            ' "' + self.local_path + self.avg_results_path + '" ' +
            ' "' + self.local_path + input_path + '" ' +
            ' "' + self.local_path + self.csv_ds_root_path + 'ID_transition_locator.csv' + '" ' +
            ' "' + self.local_path + self.csv_ds_root_path + 'ID_Rep.csv' + '" ' +
            # new
            ' "' + self.local_path + self.csv_ds_root_path + 'MetaData_PrecursorResults.csv' + '" ' +
            ' "' + self.local_path + self.final_results_dir + '" ' +
            ' "' + self.local_path + self.output().path + '" ')
        print(subprocess_call_for_r_script)

        subprocess.call(subprocess_call_for_r_script, shell=True)
