#!/usr/bin/env python3
import argparse
import json
import logging
import os
import subprocess
from distutils.dir_util import copy_tree

import utils
from pipeline import create_nextflow_folder

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def read_params():
    parser = argparse.ArgumentParser(
        description='Convert the Elyra pipeline-style JSON to a Nextflow pipeline')
    parser.add_argument(
        '-i', '--input-file', dest='input_file', required=True, help='Input JSON file representing the Elyra pipeline')
    parser.add_argument(
        '-f', '--force-create', dest='force_create', action='store_true', help='Overwrite existing output directory')

    parser.add_argument(
        '-r', '--run-pipeline', dest='run_pipeline', action='store_true', help='After creating the pipeline, run the pipeline')

    parser.add_argument(
        '-o', '--output-dir', dest='output_dir', required=True, help='Output directory where Nextflow pipeline will be written')

    parser.add_argument(
        '--run-id', dest='run_id', help='Unique identifier for each runtime pipeline', default="Unknown")
    args = parser.parse_args()
    return args


def main():
    script_dir = os.path.abspath(os.path.dirname(__file__))
    template_dir = os.path.abspath(os.path.dirname(script_dir)) + "/template"
    params = read_params()

    run_metadata = {
        'run_id': params.run_id,
        'start_time': utils.now(),
        'server_time': utils.now(),
        'status': 'prepare',
        'error_message': '',
        'log_message': ''
    }

    if os.path.exists(params.output_dir) and not params.force_create:
        logger.info(
            'Output directory exists. Please remove it before or choose another directory')
        exit(1)
    else:
        try:
            os.makedirs(params.output_dir, exist_ok=True)
        except:
            pass
        copy_tree(template_dir, params.output_dir)
        

    with open(params.input_file) as f:
        data = json.load(f)

    utils.write_to_checkpoint(params, run_metadata)
    try:
        main_nf_path = create_nextflow_folder(data, params)
        run_metadata["server_time"] = utils.now()
        run_metadata["status"] = 'prepare_success'
        utils.write_to_checkpoint(params, run_metadata)
    except Exception as e:
        run_metadata["server_time"] = utils.now()
        run_metadata["status"] = 'prepare_failure'
        run_metadata["error_message"] = str(e)
        utils.write_to_checkpoint(params, run_metadata)
        exit(1)

    if params.run_pipeline:

        run_metadata["server_time"] = utils.now()
        run_metadata["status"] = 'running'
        utils.write_to_checkpoint(params, run_metadata)
        try:
            subprocess.check_output(
                f"nextflow run {main_nf_path} -with-dag -profile conda ", shell=True)

            run_metadata["server_time"] = utils.now()
            run_metadata["status"] = 'run_success'
            utils.write_to_checkpoint(params, run_metadata)
        except Exception as e:
            run_metadata["server_time"] = utils.now()
            run_metadata["status"] = 'run_error'
            run_metadata["error_message"] = str(e)
            utils.write_to_checkpoint(params, run_metadata)
            exit(1)


if __name__ == '__main__':
    main()
