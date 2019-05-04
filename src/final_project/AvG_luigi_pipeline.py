import datetime
import luigi
import hashlib
import pprint
import subprocess
import pandas as pd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

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

class SaltString():
    @staticmethod
    def get_hash_of_file(filename, salt=''):
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
        return luigi.LocalTarget("AvG_Params.R")

class ConvertCSVToParquet(luigi.Task):
    def requires(self):
        return {'inputcsv': self.clone(InputCSVFile)}

    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input()['inputcsv'].path)
        return luigi.LocalTarget("data/AvantGardeDIA_Export_%s.parquet" % hex_tag)

    def run(self):
        chunksize = 20000
        parquet_file = self.output().path

        print(self.input()['inputcsv'].path)
        print(parquet_file)

        csv_stream = pd.read_csv(self.input()['inputcsv'].path,
                                 sep=',',
                                 chunksize=chunksize,
                                 low_memory=False)

        for i, chunk in enumerate(csv_stream):
            print("Chunk", i)
            if i == 0:
                # Guess the schema of the CSV file from the first chunk
                parquet_schema = pa.Table.from_pandas(df=chunk).schema
                # Open a Parquet file for writing
                parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            # Write CSV chunk to the parquet file
            table = pa.Table.from_pandas(chunk, schema=parquet_schema)
            parquet_writer.write_table(table)

        parquet_writer.close()


class RTask(luigi.Task):
    file_stem_rtask=luigi.Parameter(default='rtask_data')
    def requires(self):
        return MapStep()

    def output(self):
        hex_tag = SaltString.get_hash_of_file(self.input().path)
        return luigi.LocalTarget(self.file_stem_rtask + "_%s.txt" % hex_tag)

    def run(self):
        print(self.input().path)
        input_path = self.input().path

        subprocess.call('"C:/Program Files/R/R-3.4.3/bin/x64/Rscript.exe" awesome.r ' + str(input_path), shell=True)

        print("Rtask")


# echo python$PATH
# export PYTHONPATH='.'
# pipenv run luigi --module section_luigi_R RTask --local-scheduler
