import subprocess
import pandas as pd
import os


def generate_subprocess_call_for_a_analyte(hashed_id, csv_ds_root_path, params_file_path, output_dir):
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
