import hashlib
import csv
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
    """Returns the first 8 characters of the hash value of a string
    :param str or bytes some_val: thing to hash
    :param str or bytes salt: string or bytes to add randomness to the hashing,
        defaults to ''.
    :rtype: bytes
    """
    return hash_str(some_val, salt=salt).hex()[:8]

def hash_params_file(params_file):
    """Read a file as text and returns the first 8 characters of the hash of teh entire file.
    First, each row of the file is hashed and then all hashes are concatenated and hashed again.
    :param str params_file: path to the parameters file for the R script
    """
    with open(params_file, 'r') as csvfile:
        p = csv.reader(csvfile, delimiter=',')
        q = ''
        for row in p:
            q = q + hash_value(', '.join(row))
        q = hash_value(q)
        return q

def convert_csv_to_parquet_by_chunks(input_csv_path, parquet_file_path, chunksize):
    """ convert csv to parquet by chunks
        :param str input_csv_path: path to the csv file
        :param str parquet_file_path: path to the output parquet file
        :param int chunksize: size of the chunk to read

        :returns: parquet file

        """
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


def read_hashindex_and_partition_parquetFile(input_path, rootpath, csv_ds_root_path, id_analyte_path):
    """ Read the parquet file, create the hashed index for each analyte and create teh partitions
        :param str input_path: path to the parquet file
        :param str rootpath: path to the parquet dataset folder
        :param str csv_ds_root_path: path to the folder of temporary csv files
        :param str id_analyte_path: output path of the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed ids and it is used as the _SUCCESS file

        :returns: parquet dataset partitioned by the hashed id of the analyte (ID_Analyte), creates the
        'ID_Analyte_glossary' file.

        """
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

    df_transition_locator = df[['Transition Locator', 'ID_FragmentIon_charge', 'ID_Analyte']].drop_duplicates()
    df_transition_locator.to_csv(csv_ds_root_path + "ID_transition_locator.csv", index=False)

    df_ID_Analyte = df[['ID_Analyte',
                        'Protein Name',
                        'Peptide Modified Sequence',
                        'Precursor Charge',
                        'Is Decoy']].drop_duplicates()
    df_ID_Analyte.to_csv(id_analyte_path, index=False)


def read_only_one_partition_and_write_csv(parquet_dataset_dirpath, output_dirpath, ID_analyte):
    """ Read the parquet file, create the hashed index for each analyte and create teh partitions
        :param str parquet_dataset_dirpath: path to the parquet dataset folder
        :param str output_dirpath: path to the output csv file
        :param str ID_analyte: hashed id of the analyte for which the partition will be read

        :returns: csv file containing the data for a given analyte.

        """
    dataset = pq.ParquetDataset(parquet_dataset_dirpath,
                                filters=[('ID_Analyte', '=', str(ID_analyte)), ])
    df = dataset.read().to_pandas()
    df.to_csv(output_dirpath+'data_analyte_'+ID_analyte+'.csv', index=False)


def parquet_partitions_to_csvs(id_analyte_path, parquet_dataset_dirpath,output_dirpath):
    """ Converts each partition to a csv file
        :param str id_analyte_path:  path to the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed ids and it is also used as the _SUCCESS file
        :param str parquet_dataset_dirpath: path to the parquet dataset folder
        :param str output_dirpath: path to folder where the csv files will be written
        :param str ID_analyte: hashed id of the analyte for which the partition will be read

        :returns: writes a csv file for every analyte

        """
    dd = pd.read_csv(id_analyte_path)
    dd['ID_Analyte'].map(lambda x: read_only_one_partition_and_write_csv(
        parquet_dataset_dirpath=parquet_dataset_dirpath,
        output_dirpath=output_dirpath,
        ID_analyte=x))


class SaltString():
    """ calculates a hash string from the name of a file
  
        """
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
