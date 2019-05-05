import hashlib
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
