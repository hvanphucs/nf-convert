#!/usr/bin/env python3
import argparse
import json
import logging
import os
import subprocess
from distutils.dir_util import copy_tree

import db
import utils
from pipeline import create_nextflow_folder, find_execution_steps


def read_params():
    parser = argparse.ArgumentParser(
        description='Convert the Elyra pipeline-style JSON to a Nextflow pipeline')
    parser.add_argument(
        '-i', '--input-file', dest='input_file', required=False, help='Input JSON file representing the Elyra pipeline')
    parser.add_argument(
        '-f', '--force-create', dest='force_create', action='store_true', help='Overwrite existing output directory')

    parser.add_argument(
        '-r', '--run-pipeline', dest='run_pipeline', action='store_true', help='After creating the pipeline, run the pipeline')

    parser.add_argument(
        '-o', '--output-dir', dest='output_dir', required=False, help='Output directory where Nextflow pipeline will be written')

    parser.add_argument(
        '--home-dir', dest='home_dir', required=True, help='User Home directory')

    parser.add_argument(
        '--run-config', dest='run_config', required=True, help='Run config file')

    parser.add_argument(
        '--run-id', dest='run_id', help='Unique identifier for each runtime pipeline', default="Unknown")

    parser.add_argument(
        '--append-log', dest='append_log', help='Append log file for each runtime pipeline', default="run.log")
    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(
        format="[%(levelname)s][%(asctime)s][%(name)s] -- %(message)s")

    logger = logging.getLogger("ConvertPipeline")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('spam.log')
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    script_dir = os.path.abspath(os.path.dirname(__file__))
    template_dir = os.path.abspath(os.path.dirname(script_dir)) + "/template"

    params = read_params()

    with open(params.run_config, 'r') as f:
        data = json.load(f)
        # run_id:"pipeline_5hasl371j6ukshy3ycet5q"
        # run_name:"test123"
        # description:""
        # working_dir:"workflow/test5"
        # pipeline_name:"untitled2.pipeline"
        # pipeline_rev:"0"
        # status:"submitted"
        # created_at:1690792995261

        params.output_dir = os.path.join(params.home_dir, data['working_dir'])
        params.input_file = os.path.join(
            params.home_dir, data['pipeline_path'])
        params.run_id = data['run_id']

        db.update_db(data, os.path.join(params.home_dir, ".ppdb/db.json"))

    run_metadata = {
        'run_id': params.run_id,
        'start_time': utils.now(),
        'server_time': utils.now(),
        'status': 'prepare',
        'error_message': '',
        'log_message': ''
    }

    with open(params.input_file) as f:
        data = json.load(f)
        pipeline_data = data.get('pipelines')
        logger.info(f'Found  {len(pipeline_data)} pipeline data')
        try:
            pipeline_data = find_execution_steps(data)
        except Exception as e:
            logger.info(f'Failed load pipeline data', e)
            pipeline_data = pipeline_data[0].get('nodes', [])
            exit(1)

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

    utils.write_to_checkpoint(params, run_metadata)

    try:
        main_nf_path = create_nextflow_folder(pipeline_data, params, logger)
        run_metadata["server_time"] = utils.now()
        run_metadata["status"] = 'prepare_success'
        utils.write_to_checkpoint(params, run_metadata)
    except Exception as e:
        logger.error(
            f"Error when convert {e}", stack_info=True, exc_info=True)
        run_metadata["server_time"] = utils.now()
        run_metadata["status"] = 'prepare_failure'
        run_metadata["error_message"] = str(e)
        utils.write_to_checkpoint(params, run_metadata)
        exit(1)

    if params.run_pipeline:

        run_metadata["server_time"] = utils.now()
        run_metadata["status"] = 'running'
        utils.write_to_checkpoint(params, run_metadata)

        output = ""
        try:
            output = subprocess.check_output(
                f"cd {params.output_dir} && nextflow run {main_nf_path} -with-dag -profile conda >>{params.append_log} 2>&1 ", shell=True)
            run_metadata["server_time"] = utils.now()
            run_metadata["status"] = 'run_success'
            utils.write_to_checkpoint(params, run_metadata)
        except Exception as e:
            run_metadata["server_time"] = utils.now()
            run_metadata["status"] = 'run_error'
            run_metadata["error_message"] = str(e) + "\n" + str(output)
            utils.write_to_checkpoint(params, run_metadata)
            exit(1)


if __name__ == '__main__':
    main()
