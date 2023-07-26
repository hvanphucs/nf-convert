#!/usr/bin/env python3
'''
LICENSE INFO

'''

import argparse
import json
import logging
import os
import re
from distutils.dir_util import copy_tree


def get_logger():
    FORMAT = '%(asctime)s %(message)s'
    logging.basicConfig(format=FORMAT)
    logger = logging.getLogger('convert')
    logger.setLevel(logging.INFO)
    return logger


def to_camel_case(input_str):
    valid_chars = re.sub(r'[^a-zA-Z0-9]', '', input_str)
    lower_case_str = valid_chars.lower()
    words = lower_case_str.split('_')
    camel_case_words = [words[0]] + [word.capitalize() for word in words[1:]]
    camel_case_str = ''.join(camel_case_words)
    return camel_case_str


def read_params():
    parser = argparse.ArgumentParser(
        description='Convert elyra pipeline styled json to nextflow pipeline')
    parser.add_argument('-i', '--input-file', dest='input_file', required=True,
                        help='Output directory where nextflow pipeline is written')
    parser.add_argument('-f', '--force-create', dest='force_create', action='store_true',
                        help='Overwrite existing output directory')
    parser.add_argument('-o', '--output-dir', dest='output_dir', required=True,
                        help='Output directory where nextflow pipeline is written')

    args = parser.parse_args()
    return args


def find_execution_steps(graph):
    execution_steps = []
    visited_nodes = set()

    def dfs(node):
        if node["id"] not in visited_nodes:
            visited_nodes.add(node["id"])
            inputs = []
            for input_node in node["inputs"] or []:
                if input_node.get("links", None):
                    inputs.extend(input_node["links"])
            for input_link in inputs:
                from_node = get_node_by_id(graph, input_link["node_id_ref"])
                dfs(from_node)
            execution_steps.append(node)

    def get_node_by_id(graph, node_id):
        for pipeline in graph["pipelines"]:
            for node in pipeline["nodes"]:
                if node["id"] == node_id:
                    return node

    for pipeline in graph["pipelines"]:
        for node in pipeline["nodes"]:
            if node["type"] == "execution_node":
                dfs(node)

    return execution_steps


def get_home():
    return os.path.expanduser('~')


def get_full_path(path):
    if not path.startswith('/'):
        return get_home() + "/" + path
    return path


def main():
    params = read_params()
    logger = get_logger()

    script_dir = os.path.abspath(os.path.dirname(__file__))

    if os.path.exists(params.output_dir) and not params.force_create:
        logger.error(
            'Output directory exist. Please remove it before or choose other directory')
        exit(1)
    else:
        try:
            os.mkdir(params.output_dir)
        except:
            pass
        copy_tree(f'{script_dir}/template', params.output_dir)

    with open(params.input_file) as f:
        data = json.load(f)
        pipeline_data = data.get('pipelines')
        logger.info(f'Found  {len(pipeline_data)} pipeline data')
        try:
            pipeline_data = find_execution_steps(data)
        except Exception as e:
            print('Failed to load pipeline', e)
            pipeline_data = pipeline_data[0].get('nodes', [])
            exit(1)

    with open(f'{params.output_dir}/template.nf') as f:
        main_nf = f.read()

    node_param_nf = []
    node_import_nf = []
    node_workflow_nf = []

    for node in pipeline_data:
        node_id = node.get('id', None)

        node_params = node.get('app_data', {})
        for (key, value) in node_params.items():
            # print(f'{key}={value}')
            pass

        node_label = node_params.get('label', None)
        if node_label == None:
            node_label = "UnLabeled" + "_" + node_id[0:3]
        else:
            node_label = node_label + "_" + node_id[0:3]

        node_name = to_camel_case(node_label)
        node_filename = node_params.get('filename', None)
        node_runtime = node_params.get('runtime_environment', None)
        node_cpu = node_params.get('cpu', None)
        node_memory = node_params.get('memory', 4)
        node_input = node_params.get('dependencies', [])
        node_output = node_params.get('output', [])
        node_envar = node_params.get('env_vars', [])

        if node_filename is not None:
            node_filename = get_full_path(node_filename)

        process_name = node_name.upper()
        node_import_nf.append(
            'include  { process_name } from "./modules/node_name"'.replace('process_name', process_name).replace("node_name", node_name))

        node_param_nf += [
            f'params.{node_name}_filename="{node_filename}"',
            f'params.{node_name}_runtime ="{node_runtime}"',
            f'params.{node_name}_cpu="{node_cpu}"'
        ]

        for i in range(len(node_input)):
            node_param_nf.append(
                f'params.{node_name}_input{i+1}="{node_input[i]}"')
        for i in range(len(node_output)):
            node_param_nf.append(
                f'params.{node_name}_output{i+1}="{node_output[i]}"')
        node_param_nf.append("\n")
        '''
            id = a4196a91-f593-45f8-8384-49d2a220288b)
            component_parameters={}
            label=label2
            filename=pipeline/load_data.py
            runtime_environment=/miniconda/user
            cpu=100
            memory=100
            gpu=200
            gpu_vendor=100
            dependencies=['input1.txt', 'input2.txt']
            include_subdirectories=False
            output=['output1.txt', 'output2.txt']
            env_vars=[{'key': 'VAR1', 'value': '100'}]
        '''

        # write workflow submodules
        logger.info(f'[{node_name}] Write module scripts')
        with open(f"{params.output_dir}/modules/{node_name}.nf", "w") as f:
            PROCESS_TAG = '"running"'
            PROCESS_LABEL = "\n".join([
                'label "unspecific_label"'
            ])

            PROCESS_INPUT = "\n".join([
                f"path input{i+1}" for i in range(len(node_input))
            ])

            PROCESS_SCRIPT = f'''
            echo 123 > output1.txt
            echo 123 > output2.txt
            '''

            if node_filename.endswith(".ipynb"):

                filename = os.path.basename(node_filename)
                node_output.append(f'run_{filename}')

                PROCESS_SCRIPT = f'''
                    papermill \
                        -k python3 \
                        --log-output --log-level DEBUG \
                        --cwd {params.output_dir} \
                        --request-save-on-cell-execute \
                        --autosave-cell-every 10 \
                        --progress-bar \
                        {node_filename} run_{filename}
            '''
            PROCESS_OUTPUT = "\n".join([
                f'path "{node_output[i]}"' for i in range(len(node_output))
            ])
            content = '''
            process PROCESS_NAME {
                tag { PROCESS_TAG }
                PROCESS_LABEL

                conda "PROCESS_CONDA_HOME_DIR"

                input:
                PROCESS_INPUT

                output:
                PROCESS_OUTPUT

                script:
                """
                PROCESS_SCRIPT
                """
            }
            '''
            content = content.replace('PROCESS_NAME', process_name)
            content = content.replace('PROCESS_TAG', PROCESS_TAG)
            content = content.replace('PROCESS_LABEL', PROCESS_LABEL)
            content = content.replace('PROCESS_CONDA_HOME_DIR', node_runtime)
            content = content.replace('PROCESS_INPUT', PROCESS_INPUT)
            content = content.replace('PROCESS_OUTPUT', PROCESS_OUTPUT)
            content = content.replace('PROCESS_SCRIPT', PROCESS_SCRIPT)

            for line in content.split('\n'):
                line = line.strip()
                print(line, file=f)

        # write workflow chanel
        node_chanel_nf = []
        for i in range(len(node_input)):
            node_chanel_nf.append(
                f'{node_name}_chanel_input{i+1}=Channel.fromPath(params.{node_name}_input{i+1}).toSortedList()')

        node_chanel_nf_name = [
            f'{node_name}_chanel_input{i+1}' for i in range(len(node_input))]
        node_process_rf = f'''{process_name}({", ".join(node_chanel_nf_name)})'''
        node_workflow_nf += [
            "\n".join(node_chanel_nf),
            node_process_rf
        ]

    main_nf = main_nf.replace(
        '/*>>>>>[PARAMS DEFINE SECTION]*/', "\n".join(node_param_nf))
    main_nf = main_nf.replace(
        '/*>>>>>[IMPORT MODULES SECTION]*/ ', "\n".join(node_import_nf))

    main_nf = main_nf.replace(
        '/*>>>>>[COMPOSE WORFLOW ]*/', "\n\n".join(node_workflow_nf))

    with open(f'{params.output_dir}/main.nf', 'w') as f:
        print(main_nf, file=f)

    try:
        os.remove(f'{params.output_dir}/template.nf')
    except:
        pass

    logger.info(f'Finished processing. Ouput: {params.output_dir}')


if __name__ == '__main__':

    main()
