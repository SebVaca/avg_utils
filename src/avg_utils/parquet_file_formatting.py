import hashlib
import csv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import string
import os
import zipfile


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

def read_by_rowgroup_hashindex_and_partition_parquetFile(input_path, rootpath, csv_ds_root_path, id_analyte_path):
    """ Read the parquet file, create the hashed index for each analyte and create teh partitions
        :param str input_path: path to the parquet file
        :param str rootpath: path to the parquet dataset folder
        :param str csv_ds_root_path: path to the folder of temporary csv files
        :param str id_analyte_path: output path of the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed ids and it is used as the _SUCCESS file

        :returns: parquet dataset partitioned by the hashed id of the analyte (ID_Analyte), creates the
        'ID_Analyte_glossary' file.

        """
    f = pq.ParquetFile(source=input_path)
    for i in range(f.num_row_groups):
        df = f.read_row_group(i).to_pandas()

        # Concatenate values from multiple columns to create a unique identifier for each
        # Analyte, transition (signal) and MS acquisition (Mass spectrometry analysis)

        df['ID_Analyte'] = df['Protein Name'].astype(str) + '_' + df['Peptide Modified Sequence'].astype(str) + '_' + \
                           df['Precursor Charge'].astype(str) + df['Is Decoy'].astype(str)
        df['ID_FragmentIon_charge'] = df['Fragment Ion'].astype(str) + '_' + df['Product Charge'].astype(str)

        # Hashed the values to obtain the unique identifier
        df['ID_Analyte'] = df['ID_Analyte'].map(lambda x: hash_value(x))
        df['ID_FragmentIon_charge'] = df['ID_FragmentIon_charge'].map(lambda x: hash_value(x))
        df['ID_Rep'] = df['File Name'].astype(str).map(lambda x: hash_value(x))

        # Correct types of columns

        df = df.astype({'Protein Name': 'object', 'Peptide Modified Sequence': 'object', 'Modified Sequence': 'object',
                    'Isotope Label Type': 'object', 'Is Decoy': 'bool', 'Precursor Charge': 'int64',
                    'Precursor Mz': 'float64', 'Product Charge': 'int64', 'Product Mz': 'float64',
                    'Fragment Ion': 'object', 'Transition Locator': 'object', 'Quantitative': 'bool',
                    'File Name': 'object', 'Library Dot Product': 'float64', 'Min Start Time': 'float64',
                    'Max End Time': 'float64', 'Area': 'float64', 'Library Intensity': 'float64',
                    'Interpolated Times': 'object', 'Interpolated Intensities': 'object',
                    'Interpolated Mass Errors': 'object', 'Precursor Result Locator': 'object',
                    'ID_FragmentIon_charge': 'object', 'ID_Rep': 'object'})

        fields = [pa.field('Protein Name', pa.string()),
                  pa.field('Peptide Modified Sequence', pa.string()),
                  pa.field('Modified Sequence', pa.string()),
                  pa.field('Isotope Label Type', pa.string()),
                  pa.field('Is Decoy', pa.bool_()),
                  pa.field('Precursor Charge', pa.int64()),
                  pa.field('Precursor Mz', pa.float64()),
                  pa.field('Product Charge', pa.int64()),
                  pa.field('Product Mz', pa.float64()),
                  pa.field('Fragment Ion', pa.string()),
                  pa.field('Transition Locator', pa.string()),
                  pa.field('Quantitative', pa.bool_()),
                  pa.field('File Name', pa.string()),
                  pa.field('Library Dot Product', pa.float64()),
                  pa.field('Min Start Time', pa.float64()),
                  pa.field('Max End Time', pa.float64()),
                  pa.field('Area', pa.float64()),
                  pa.field('Library Intensity', pa.float64()),
                  pa.field('Interpolated Times', pa.string()),
                  pa.field('Interpolated Intensities', pa.string()),
                  pa.field('Interpolated Mass Errors', pa.string()),
                  pa.field('Precursor Result Locator', pa.string()),
                  pa.field('ID_FragmentIon_charge', pa.string()),
                  pa.field('ID_Rep', pa.string()),
                  pa.field('ID_Analyte', pa.string()),
                  ]

        my_schema = pa.schema(fields)

        table = pa.Table.from_pandas(df, my_schema)
        pq.write_to_dataset(table,
                            root_path=rootpath,
                            partition_cols=['ID_Analyte'])

        # Create directory files to save the correspondance of the hash ids and the real values
        def write_to_same_csv_appending(dt, j, path, filename=None):

            if filename is not None:
                path_file = path + filename
            else:
                path_file = path

            if j == 0:
                dt.to_csv(path_file,
                          index=False,
                          header=True)
            else:
                dt.to_csv(path_file,
                          mode='a',
                          index=False,
                          header=False)

        df_ID_FragmentIon_charge = df[['ID_FragmentIon_charge',
                                       'Fragment Ion',
                                       'Product Charge']].drop_duplicates()
        write_to_same_csv_appending(df_ID_FragmentIon_charge, i, csv_ds_root_path, "ID_FragmentIon_charge.csv")


        df_ID_Rep = df[['ID_Rep', 'File Name']].drop_duplicates()
        write_to_same_csv_appending(df_ID_Rep, i, csv_ds_root_path, "ID_Rep.csv")

        df_transition_locator = df[['Transition Locator', 'ID_FragmentIon_charge', 'ID_Analyte']].drop_duplicates()
        write_to_same_csv_appending(df_transition_locator, i, csv_ds_root_path, "ID_transition_locator.csv")
        # new
        df_ID_PrecursorResult = df[['Precursor Result Locator',
                            'Protein Name',
                            'Peptide Modified Sequence',
                            'Isotope Label Type',
                            'Precursor Charge',
                            'Is Decoy',
                            'File Name']].drop_duplicates()
        write_to_same_csv_appending(df_ID_PrecursorResult, i, csv_ds_root_path, "MetaData_PrecursorResults.csv")

        df_ID_Analyte = df[['ID_Analyte',
                            'Protein Name',
                            'Peptide Modified Sequence',
                            'Precursor Charge',
                            'Is Decoy']].drop_duplicates()
        write_to_same_csv_appending(df_ID_Analyte, i, id_analyte_path)

    # read, drop duplicates and write file again
    def read_drop_duplicates_rewrite(path):
        pd.read_csv(path).drop_duplicates().to_csv(path, index=False, header=True)

    read_drop_duplicates_rewrite(csv_ds_root_path + "ID_FragmentIon_charge.csv")
    read_drop_duplicates_rewrite(csv_ds_root_path + "ID_Rep.csv")
    read_drop_duplicates_rewrite(csv_ds_root_path + "ID_transition_locator.csv")
    # new
    read_drop_duplicates_rewrite(csv_ds_root_path + "MetaData_PrecursorResults.csv")
    read_drop_duplicates_rewrite(id_analyte_path)



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
    # df.to_csv(output_dirpath+'data_analyte_'+ID_analyte+'.csv', index=False)
    df.to_csv(os.path.join(output_dirpath, 'data_analyte_'+ID_analyte+'.csv'), index=False)


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


def read_csv_by_chunks_createindices_and_partitionPQbygroup(input_csv_path,
                                                            parquet_dataset_output_path,
                                                            indices_csv_output_path,
                                                            chunksize,
                                                            n_parts):
    """ convert csv to parquet by chunks
        :param str input_csv_path: path to the csv file
        :param str parquet_dataset_path: path to the output parquet file
        :param int chunksize: size of the chunk to read
        :param int n_parts: number of partitions
        :returns: parquet file
        """
    csv_stream = pd.read_csv(input_csv_path,
                             sep=',',
                             chunksize=chunksize,
                             low_memory=False)

    fields = [pa.field('Protein Name', pa.string()),
              pa.field('Peptide Modified Sequence', pa.string()),
              pa.field('Modified Sequence', pa.string()),
              pa.field('Isotope Label Type', pa.string()),
              pa.field('Is Decoy', pa.bool_()),
              pa.field('Precursor Charge', pa.int64()),
              pa.field('Precursor Mz', pa.float64()),
              pa.field('Product Charge', pa.int64()),
              pa.field('Product Mz', pa.float64()),
              pa.field('Fragment Ion', pa.string()),
              pa.field('Transition Locator', pa.string()),
              pa.field('Quantitative', pa.bool_()),
              pa.field('File Name', pa.string()),
              pa.field('Library Dot Product', pa.float64()),
              pa.field('Min Start Time', pa.float64()),
              pa.field('Max End Time', pa.float64()),
              pa.field('Area', pa.float64()),
              pa.field('Library Intensity', pa.float64()),
              pa.field('Interpolated Times', pa.string()),
              pa.field('Interpolated Intensities', pa.string()),
              pa.field('Interpolated Mass Errors', pa.string()),
              pa.field('Precursor Result Locator', pa.string()),
              pa.field('ID_FragmentIon_charge', pa.string()),
              pa.field('ID_Rep', pa.string()),
              pa.field('ID_Analyte', pa.string()),
              pa.field('ID_group', pa.int64()),
              ]

    my_schema = pa.schema(fields)


    for i, chunk in enumerate(csv_stream):
        print("Chunk", i)
        df_annotated = create_indices(chunk, n_parts)
        table = pa.Table.from_pandas(df=df_annotated, schema=my_schema)
        pq.write_to_dataset(table,
                            root_path=parquet_dataset_output_path,
                            partition_cols=['ID_group', 'ID_Analyte'])

        df = df_annotated

        # Create directory files to save the correspondance of the hash ids and the real values
        def write_to_same_csv_appending(dt, j, path, filename=None):

            if filename is not None:
                path_file = os.path.join(path, filename)
            else:
                path_file = path

            if j == 0:
                dt.to_csv(path_file,
                          index=False,
                          header=True)
            else:
                dt.to_csv(path_file,
                          mode='a',
                          index=False,
                          header=False)

        df_ID_FragmentIon_charge = df[['ID_FragmentIon_charge',
                                       'Fragment Ion',
                                       'Product Charge']].drop_duplicates()
        write_to_same_csv_appending(df_ID_FragmentIon_charge, i, indices_csv_output_path, "ID_FragmentIon_charge.csv")

        df_ID_Rep = df[['ID_Rep', 'File Name']].drop_duplicates()
        write_to_same_csv_appending(df_ID_Rep, i, indices_csv_output_path, "ID_Rep.csv")

        df_transition_locator = df[['Transition Locator', 'ID_FragmentIon_charge', 'ID_Analyte']].drop_duplicates()
        write_to_same_csv_appending(df_transition_locator, i, indices_csv_output_path, "ID_transition_locator.csv")

        df_ID_Analyte = df[['ID_Analyte',
                            'Protein Name',
                            'Peptide Modified Sequence',
                            'Precursor Charge',
                            'Is Decoy']].drop_duplicates()
        write_to_same_csv_appending(df_ID_Analyte, i, indices_csv_output_path, "ID_Analyte.csv")

        df_ID_Analyte_withgroup = df[['ID_Analyte',
                            'Protein Name',
                            'Peptide Modified Sequence',
                            'Precursor Charge',
                            'Is Decoy',
                            'ID_group']].drop_duplicates()
        write_to_same_csv_appending(df_ID_Analyte_withgroup, i, indices_csv_output_path, "ID_Analyte_withgroup.csv")

        #new
        df_ID_PrecursorResult = df[['Precursor Result Locator',
                                    'Protein Name',
                                    'Peptide Modified Sequence',
                                    'Isotope Label Type',
                                    'Precursor Charge',
                                    'Is Decoy',
                                    'File Name']].drop_duplicates()
        write_to_same_csv_appending(df_ID_PrecursorResult, i, indices_csv_output_path, "MetaData_PrecursorResults.csv")

    # read, drop duplicates and write file again
    def read_drop_duplicates_rewrite(path):
        pd.read_csv(path).drop_duplicates().to_csv(path, index=False, header=True)

    read_drop_duplicates_rewrite(os.path.join(indices_csv_output_path, "ID_FragmentIon_charge.csv"))
    read_drop_duplicates_rewrite(os.path.join(indices_csv_output_path, "ID_Rep.csv"))
    read_drop_duplicates_rewrite(os.path.join(indices_csv_output_path, "ID_transition_locator.csv"))
    read_drop_duplicates_rewrite(os.path.join(indices_csv_output_path, "ID_Analyte.csv"))
    read_drop_duplicates_rewrite(os.path.join(indices_csv_output_path, "ID_Analyte_withgroup.csv"))
    # new
    read_drop_duplicates_rewrite(os.path.join(indices_csv_output_path, "MetaData_PrecursorResults.csv"))




def create_indices(pandas_df, n_parts):

    df = pandas_df
    # Concatenate values from multiple columns to create a unique identifier for each
    # Analyte, transition (signal) and MS acquisition (Mass spectrometry analysis)

    df['ID_Analyte'] = df['Protein Name'].astype(str) + '_' + df['Peptide Modified Sequence'].astype(str) + '_' + \
                       df['Precursor Charge'].astype(str) + df['Is Decoy'].astype(str)
    df['ID_FragmentIon_charge'] = df['Fragment Ion'].astype(str) + '_' + df['Product Charge'].astype(str)

    # Hashed the values to obtain the unique identifier
    df['ID_Analyte'] = df['ID_Analyte'].map(lambda x: hash_value(x))
    df['ID_FragmentIon_charge'] = df['ID_FragmentIon_charge'].map(lambda x: hash_value(x))
    df['ID_Rep'] = df['File Name'].astype(str).map(lambda x: hash_value(x))

    # obtain dictionary of groups from first two characters of the hash value of the ID_analyte
    combs_dict = hashvalue_to_groupnumber_dictionary(n_parts)

    df['ID_group'] = df['ID_Analyte'].map(lambda x: str(x[0:2]))
    df['ID_group'] = df['ID_group'].map(combs_dict)
    return df


def hashvalue_to_groupnumber_dictionary(n_parts):

    # list of all characters used in sha 256.
    alphanum = string.ascii_lowercase[0:6] + string.digits

    # all possible combinations of two characters
    combs = [val1 + val2 for val1 in alphanum for val2 in alphanum]

    combs_df = pd.DataFrame(combs, columns=['combs'])
    combs_df['number'] = combs_df.index
    combs_df['cat'] = pd.cut(combs_df['number'], n_parts)
    combs_df['group'] = pd.factorize(combs_df['cat'])[0] + 1
    combs_df2 = combs_df[['combs', 'group']]
    combs_dict = dict(zip(combs_df2.combs, combs_df2.group))

    return combs_dict


def zip_dir_keeping_folder_structure(directory, zipname):
    """ Compress a directory (ZIP file).
        :param str directory: path to the directory to zip (e.g '/path/to/folder/')
        :param str zipname: path to the output zip file (e.g. '/path/to/zipfile.zip')
    """
    print(os.path.exists(directory))
    if os.path.exists(directory):
        outZipFile = zipfile.ZipFile(zipname, 'w', zipfile.ZIP_DEFLATED)

        # The root directory within the ZIP file.
        rootdir = os.path.basename(directory)

        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:

                # Write the file named filename to the archive,
                # giving it the archive name 'arcname'.
                filepath   = os.path.join(dirpath, filename)
                parentpath = os.path.relpath(filepath, directory)
                arcname    = os.path.join(rootdir, parentpath)

                outZipFile.write(filepath, arcname)

    outZipFile.close()


def zip_ParquetPartitions_individually(parquet_dataset_path,
                                               zip_outputs):
    """ zips top-level parquet dataset partition individually
        :param str parquet_dataset_path: path to the parquet dataset (e.g '/path/to/PQ_dataset/')
        :param str zip_outputs: path to the folder which will containt the zip files (e.g. '/path/to/folder/')
    """
    #dirs = [d for d in os.listdir(reports_path) if os.path.isdir(os.path.join(reports_path, d))]
    dirs = [os.path.join(parquet_dataset_path, d) for d in os.listdir(parquet_dataset_path) if
            os.path.isdir(os.path.join(parquet_dataset_path, d))]
    for d in dirs:
        file_path = os.path.join(zip_outputs, os.path.basename(d) + '.zip')
        file_path = os.path.abspath(os.path.normpath(os.path.expanduser(file_path)))
        d = os.path.abspath(os.path.normpath(os.path.expanduser(d)))

        print('Compressing folder : ' + d)
        zip_dir_keeping_folder_structure(d, file_path)
        print('Created zip file to: ' + file_path)


def unzip_ParquetPartition_keepingDatasetstructure(zip_filepath, zip_output_path):
    """ unzips the zip file containing one partition of the parquet dataset and keeps the dataset structure
        :param str zip_filepath: path to the zip file to unzip (e.g '/path/to/zipfile.zip')
        :param str zip_output_path: path to the output zip file (e.g. '/path/to/output/')
    """
    # a = os.path.basename('C:/Users/svaca/Desktop/Temps/PQ_partition_develop/zip_files/ID_group=9.zip')
    # a = os.path.splitext(a)[0]

    # Create a ZipFile Object and load sample.zip in it
    with zipfile.ZipFile(zip_filepath, 'r') as zipObj:
       # Extract all the contents of zip file in different directory
       zipObj.extractall(zip_output_path)

def create_path_of_PQpartition(path):
    """ unzips the zip file containing one partition of the parquet dataset and keeps the dataset structure
        :param str zip_filepath: path to the zip file to unzip (e.g '/path/to/zipfile.zip')
        :param str zip_output_path: path to the output zip file (e.g. '/path/to/output/')
    """
    a = os.listdir(path)[0]
    b = os.path.join(path, a)

    return b

def parquet_to_csvs_from_one_partition(path_PQ_partition_byGroup, parquet_dataset_dirpath,output_dirpath):
    """ Converts each partition to a csv file
        :param str id_analyte_path:  path to the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed ids and it is also used as the _SUCCESS file
        :param str parquet_dataset_dirpath: path to the parquet dataset folder
        :param str output_dirpath: path to folder where the csv files will be written
        :param str ID_analyte: hashed id of the analyte for which the partition will be read

        :returns: writes a csv file for every analyte

        """
    dd = [w.replace('ID_Analyte=', '') for w in os.listdir(path_PQ_partition_byGroup)]
    dd = pd.DataFrame(dd, columns=['ID_Analyte'])
    dd['ID_Analyte'].map(lambda x: read_only_one_partition_and_write_csv(
        parquet_dataset_dirpath=parquet_dataset_dirpath,
        output_dirpath=output_dirpath,
        ID_analyte=x))
