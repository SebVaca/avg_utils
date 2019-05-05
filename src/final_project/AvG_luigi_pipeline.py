import datetime
import os
import luigi
import hashlib
import pprint
import subprocess
import pandas as pd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def hash_str(some_val, salt=''):
    """Converts strings to hash digest
    See: https://en.wikipedia.org/wiki/Salt_(cryptography)
    :param str or bytes some_val: thing to hash
    :param str or bytes salt: string or bytes to add randomness to the hashing,
        defaults to ''.
    :rtype: bytes
    """
    if isinstance(salt, bytes):
        hashed_val = hashlib.sha256(salt + str.encode(some_val))

    if isinstance(salt, str):
        hashed_val = hashlib.sha256(str.encode(salt + some_val))
    return hashed_val.digest()

def hash_value(some_val, salt=''):
    return hash_str(some_val, salt=salt).hex()[:8]

def read_hashindex_and_partition_parquetFile(input_path, rootpath, csv_ds_root_path, id_analyte_path):

    df = pq.read_pandas(input_path).to_pandas()

    # Concatenate values from multiple columns to create a unique identifier for each
    # Analyte, transition (signal) and MS acquisition (Mass spectrometry analysis)

    df['ID_Analyte'] = df['Protein Name'].astype(str) + '_' + df['Peptide Modified Sequence'].astype(str) + '_' + \
                       df['Precursor Charge'].astype(str) + df['Is Decoy'].astype(str)
    df['ID_FragmentIon_charge'] = df['Fragment Ion'].astype(str) + '_' + df['Product Charge'].astype(str)

    # Hashed the values to obtain the unique identifier
    df['ID_Analyte'] = df['ID_Analyte'].map(lambda x: hash_value(x))
    df['ID_FragmentIon_charge'] = df['ID_FragmentIon_charge'].map(lambda x: hash_value(x))
    df['ID_Rep'] = df['File Name'].astype(str).map(lambda x: hash_value(x))

    table = pa.Table.from_pandas(df)
    pq.write_to_dataset(table,
                        root_path=rootpath,
                        partition_cols=['ID_Analyte'])

    ## Create directory files to save the correspondance of the hash ids and the real values
    df_ID_FragmentIon_charge = df[['ID_FragmentIon_charge',
                                   'Fragment Ion',
                                   'Product Charge']].drop_duplicates()
    df_ID_FragmentIon_charge.to_csv(csv_ds_root_path + "ID_FragmentIon_charge.csv", index=False)

    df_ID_Rep = df[['ID_Rep', 'File Name']].drop_duplicates()
    df_ID_Rep.to_csv(csv_ds_root_path + "ID_Rep.csv", index=False)

    df_ID_Analyte = df[['ID_Analyte',
                        'Protein Name',
                        'Peptide Modified Sequence',
                        'Precursor Charge',
                        'Is Decoy']].drop_duplicates()
    df_ID_Analyte.to_csv(id_analyte_path, index=False)


def convert_csv_to_parquet_by_chunks(input_csv_path, parquet_file_path, chunksize):
    csv_stream = pd.read_csv(input_csv_path,
                             sep=',',
                             chunksize=chunksize,
                             low_memory=False)

    for i, chunk in enumerate(csv_stream):
        print("Chunk", i)
        if i == 0:
            # Guess the schema of the CSV file from the first chunk
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            # Open a Parquet file for writing
            parquet_writer = pq.ParquetWriter(parquet_file_path, parquet_schema, compression='snappy')
        # Write CSV chunk to the parquet file
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)
        parquet_writer.close()


def read_only_one_partition_and_write_csv(parquet_dataset_dirpath, output_dirpath, ID_analyte):
    dataset = pq.ParquetDataset(parquet_dataset_dirpath,
                                filters=[('ID_Analyte', '=', str(ID_analyte)), ])
    df = dataset.read().to_pandas()
    df.to_csv(output_dirpath+'ID_Analyte_'+ID_analyte+'.csv', index=False)


def parquet_partitions_to_csvs(id_analyte_path, parquet_dataset_dirpath,output_dirpath):
    dd = pd.read_csv(id_analyte_path)
    dd['ID_Analyte'].map(lambda x: read_only_one_partition_and_write_csv(
        parquet_dataset_dirpath=parquet_dataset_dirpath,
        output_dirpath=output_dirpath,
        ID_analyte=x))


class SaltString():
    @staticmethod
    def get_hash_of_file(filename):
        try:
            hasher = hashlib.md5()
            with open(filename, 'rb') as input_file:
                buf = input_file.read()
                hasher.update(buf)
            return hasher.hexdigest()[:8]
        except:
            print("file not yet created")



class InputCSVFile(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(
            "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example/AvantGardeDIA_Export.csv")

class ParamsFile(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(
            "C:/Users/Sebastian Vaca/PycharmProjects/Hardvard_Ext/Project/AvG_Example/AvG_Params.R")

class ConvertCSVToParquet(luigi.Task):
    def requires(self):
        return {'inputcsv': self.clone(InputCSVFile)}

    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input()['inputcsv'].path)
        return luigi.LocalTarget("data/AvantGardeDIA_Export_%s.parquet" % hex_tag)

    def run(self):
        convert_csv_to_parquet_by_chunks(input_csv_path=self.input()['inputcsv'].path,
                                         parquet_file_path=self.output().path,
                                         chunksize=20000)

class ReadHashIndexAndPartitionParquetFile(luigi.Task):
    root_path = luigi.Parameter(default='data/pq_ds/', is_global=True)
    csv_ds_root_path = luigi.Parameter(default='data/csv_ds/', is_global=True)

    def requires(self):
        return ConvertCSVToParquet()
    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input().path)
        return luigi.LocalTarget(self.csv_ds_root_path+'ID_Analyte_glossary'+"_%s.csv" % hex_tag)
    def run(self):
        read_hashindex_and_partition_parquetFile(input_path=self.input().path,
                                                 rootpath=self.root_path,
                                                 csv_ds_root_path=self.csv_ds_root_path,
                                                 id_analyte_path=self.output().path)


class TransformParquetPartitionsToCSV(luigi.Task):
    parquet_dataset_dirpath = ReadHashIndexAndPartitionParquetFile.root_path
    csv_ds_root_path = ReadHashIndexAndPartitionParquetFile.csv_ds_root_path

    def requires(self):
        return ReadHashIndexAndPartitionParquetFile()
    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input().path)
        return luigi.LocalTarget(self.csv_ds_root_path+'ID_Analyte_glossary_2'+"_%s.csv" % hex_tag)
    def run(self):
        parquet_partitions_to_csvs(id_analyte_path=self.input().path,
                                   parquet_dataset_dirpath=self.parquet_dataset_dirpath,
                                   output_dirpath=self.csv_ds_root_path)
        df = pd.read_csv(self.input().path)
        df.to_csv(self.output().path)


class RTask_AvantGarde(luigi.Task):
    file_stem_rtask=luigi.Parameter(default='rtask_data')
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

        # run_r_script_for_an_analyte(hashed_id='0c638508', csv_ds_root_path=self.csv_ds_root_path,
        #                             params_file_path=params_file_path,
        #                             output_dir=self.output_dir)

        run_r_script_for_all_analytes(id_analyte_path=input_path,
                                      csv_ds_root_path=self.csv_ds_root_path,
                                      params_file_path=params_file_path,
                                      output_dir=self.output_dir)

        df = pd.read_csv(input_path)
        df.to_csv(self.output().path)



# echo python$PATH
# export PYTHONPATH='.'
# pipenv run luigi --module section_luigi_R RTask --local-scheduler

def generate_subprocess_call_for_a_analyte(hashed_id, csv_ds_root_path, params_file_path, output_dir):
    R_SCRIPT_PATH = os.getenv('R_SCRIPT_PATH')
    local_path = os.getenv('local_path')

    subprocess_call_for_r_script = str(
        R_SCRIPT_PATH +
        ' "' + local_path + 'src/AvG_R_scripts/AvG_from_partitionedParquet.R' + '" ' +
        ' "' + local_path + csv_ds_root_path + 'ID_Analyte_' + str(hashed_id) + '.csv' + '" ' +
        ' "' + str(params_file_path) + '" ' +
        ' "' + str(hashed_id) + '" ' +
        ' "' + local_path + output_dir + '" ')

    return subprocess_call_for_r_script

def run_r_script_for_an_analyte(hashed_id, csv_ds_root_path, params_file_path, output_dir):

    subprocess_call_for_r_script = generate_subprocess_call_for_a_analyte(
        hashed_id, csv_ds_root_path, params_file_path, output_dir)

    # print('subcall:' + subprocess_call_for_r_script)

    subprocess.call(subprocess_call_for_r_script, shell=True)

def run_r_script_for_all_analytes(id_analyte_path,csv_ds_root_path, params_file_path, output_dir):
    dd = pd.read_csv(id_analyte_path)
    dd['ID_Analyte'].map(lambda x: run_r_script_for_an_analyte(
        hashed_id=x,
        csv_ds_root_path=csv_ds_root_path,
        params_file_path=params_file_path,
        output_dir=output_dir))

