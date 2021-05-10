import subprocess
import pandas as pd
import os
import multiprocessing as mp


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

    R_SCRIPT_PATH = "Rscript"

    subprocess_call_for_r_script = str(
        R_SCRIPT_PATH +
        ' "/usr/local/src/AvG_for_Terra.R" ' +
        ' "' + os.path.join(csv_ds_root_path, 'data_analyte_' + str(hashed_id) + '.csv') + '" ' +
        ' "' + str(params_file_path) + '" ' +
        ' "' + str(hashed_id) + '" ' +
        ' "' + str(output_dir) + '" ')

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


def run_r_script_for_all_analytes(csv_ds_root_path, params_file_path, output_dir):
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
    dd = [w.replace('data_analyte_', '') for w in os.listdir(csv_ds_root_path)]
    dd = [w.replace('.csv', '') for w in dd]
    dd = pd.DataFrame(dd, columns=['ID_Analyte'])
    # dd['ID_Analyte'].map(lambda x: run_r_script_for_an_analyte(
    #     hashed_id=x,
    #     csv_ds_root_path=csv_ds_root_path,
    #     params_file_path=params_file_path,
    #     output_dir=output_dir))

    pool = mp.Pool(mp.cpu_count())
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print(mp.cpu_count())
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    num_analytes = len(dd['ID_Analyte'])
    pool.starmap(run_r_script_for_an_analyte,
                 [(str(dd['ID_Analyte'][i]), csv_ds_root_path, params_file_path, output_dir) for i in range(num_analytes)])

