import subprocess
import pandas as pd
import os


def generate_subprocess_call_for_a_analyte(hashed_id, csv_ds_root_path, params_file_path, output_dir):
    """ Generates the command to be passed to the subprocess module.
        The command is created for a given analyte.
        :param str hashed_id: hashed id of the analyte
        :param str csv_ds_root_path: path to the folder of temporary csv files
        :param str params_file: path to the parameters file for the R script
        :param str output_dir: output path of the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed ids and it is used as the _SUCCESS file

        :returns: subprocess_call_for_r_script

        """

    R_SCRIPT_PATH = os.getenv('R_SCRIPT_PATH')
    local_path = os.getenv('local_path')

    subprocess_call_for_r_script = str(
        R_SCRIPT_PATH +
        ' "' + local_path + 'src/AvG_R_scripts/AvG_from_partitionedParquet.R' + '" ' +
        ' "' + local_path + csv_ds_root_path + 'data_analyte_' + str(hashed_id) + '.csv' + '" ' +
        ' "' + str(params_file_path) + '" ' +
        ' "' + str(hashed_id) + '" ' +
        ' "' + local_path + output_dir + '" ')

    return subprocess_call_for_r_script

def run_r_script_for_an_analyte(hashed_id, csv_ds_root_path, params_file_path, output_dir):
    """ Generates a command and passes it to the subprocess module.
        The R script is run using the data for a analyte.

        :param str hashed_id: hashed id of the analyte
        :param str csv_ds_root_path: path to the folder of temporary csv files
        :param str params_file: path to the parameters file for the R script
        :param str output_dir: output path of the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed ids and it is used as the _SUCCESS file


        """

    subprocess_call_for_r_script = generate_subprocess_call_for_a_analyte(
        hashed_id, csv_ds_root_path, params_file_path, output_dir)

    # print('subcall:' + subprocess_call_for_r_script)

    subprocess.call(subprocess_call_for_r_script, shell=True)

def run_r_script_for_all_analytes(id_analyte_path,csv_ds_root_path, params_file_path, output_dir):
    """ Run the avant-garde R script for all analytes. Reads the 'ID_Analyte_glossary' file, that contains all
    hashed id values for all the analytes and iterates through it.

        :param str id_analyte_path: output path of the 'ID_Analyte_glossary' file. This file contains the values of all
        hashed id values.
        :param str hashed_id: hashed id of the analyte
        :param str csv_ds_root_path: path to the folder of temporary csv files
        :param str params_file_path: path to the parameters file for the R script
        :param str output_dir: output path of the 'ID_Analyte_glossary' file that is used as a _SUCCESS file
        to be written in R to make sure that the script ran without errors.

        """
    dd = pd.read_csv(id_analyte_path)
    dd['ID_Analyte'].map(lambda x: run_r_script_for_an_analyte(
        hashed_id=x,
        csv_ds_root_path=csv_ds_root_path,
        params_file_path=params_file_path,
        output_dir=output_dir))
