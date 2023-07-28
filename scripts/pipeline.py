#!/usr/bin/env python3
import json
import logging
import os
import shutil
import subprocess

import kernel
import utils



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


def get_node_by_id(node_list, id):
    for node in node_list:
        if node["id"] == id:
            return node
    return None


def get_node_name(node):
    node_id = node.get('id', None)
    node_params = node.get('app_data', {})
    node_label = node_params.get('label', None)
    node_filename = node_params.get('filename', None)
    if node_filename is not None:
        node_filename = utils.get_full_path(node_filename)

    if node_label == None or len(node_label.strip()) == 0:
        basename = os.path.basename(node_filename).split(".")[0]
        node_label = f"{basename}" + "_" + node_id[0:3]
    else:
        node_label = node_label + "_" + node_id[0:3]

    node_name = utils.to_camel_case(node_label)
    return node_name


def get_node_process_label(node):
    node_name = get_node_name(node)
    process_name = node_name.upper()
    return process_name


def format_print_node(node):
    ignoreKey = ["ui_data"]
    beautyNode = {}
    for key, value in node.items():
        if not key in ignoreKey:
            beautyNode[key] = value
    return beautyNode


def create_nextflow_folder(pipeline_data, params, logger):

    with open(f'{params.output_dir}/template.nf') as f:
        main_nf = f.read()

    node_param_nf = []
    node_import_nf = []
    node_workflow_nf = []

    for node in pipeline_data:
        print("!23312312")
        node_id = node.get('id', None)
        node_type = node.get('type', None)  # execution_node
        node_group = node.get('op', None)  # notebook-node

        logger.info("\n----Process node-----")
        logger.info(format_print_node(node))

        if node_type != 'execution_node' or node_group is None:
            logger.warning(
                f"Ignore this node because node_type={node_type} and node_group={node_group}")
            continue

        node_params = node.get('app_data', {})
        node_filename = node_params.get('filename', None)
        node_runtime = node_params.get('runtime_environment', None)
        node_runtime_yaml = node_params.get('environment_yaml', '')
        node_cpu = node_params.get('cpu', None)
        node_memory = node_params.get('memory', 4)
        node_input = [i for i in node_params.get(
            'dependencies', []) if len(i.strip()) > 0]
        node_output = [i for i in node_params.get(
            'output', []) if len(i.strip()) > 0]
        node_envar = [i for i in node_params.get(
            'env_vars', [])]

        if node_filename is not None:
            node_filename = utils.get_full_path(node_filename)

        node_name = get_node_name(node)
        process_name = get_node_process_label(node)

        if node_runtime == '' and len(node_runtime_yaml) > 10:
            node_runtime_new_env = f'{node_name}_env'
            node_runtime_yaml_file = f'{params.output_dir}/env/{node_name}_env.yaml'

            with open(node_runtime_yaml_file, "w") as env_yaml_file:
                print(node_runtime_yaml.strip(), file=env_yaml_file)

            try:
                subprocess.check_output(
                    f'''conda env remove -y --name {node_runtime_new_env} | | true && \
                    && mamba env create -y -n {node_runtime_new_env} -f {node_runtime_yaml_file} 
                ''')
            except Exception as e:
                logger.info(f'Failed to create node runtime: {e}')

            node_runtime_new_env_path = kernel.get_conda_env_path(
                node_runtime_new_env)
            if node_runtime_new_env_path is not None:
                node_runtime = node_runtime_new_env_path

        node_import_nf.append(
            'include  { PROCESS_NAME } from "./modules/NODE_NAME"'.replace('PROCESS_NAME', process_name).replace("NODE_NAME", node_name))

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
            echo "The output of the process is unknown."
            '''

            if node_filename.endswith(".ipynb"):
                filename = os.path.basename(node_filename)
                output_notebook = f"run_{filename}"
                node_output.append(output_notebook)

                logger.info(
                    f'[Validate notebook] {filename}')

                node_params = [
                    f"-p {param['key']} '{param['value']}'" for param in node_envar]

                if node_group == 'notebook-node':
                    logger.info('Detected node is notebook-node')
                    with open(node_filename) as fnotebook:
                        notebook = json.load(fnotebook)

                    language = notebook.get(
                        'metadata', {}).get('kernelspec', {}).get('language', {})
                    logger.info(
                        f'[Validate notebook] Detected language:  {language}')
                    if language == "R":
                        kernel_name, env_location = kernel.prepare_kernel(
                            node_runtime, "python")
                        logger.info(
                            f"Notebook using kernel: {kernel_name} with environment {env_location}")
                    elif language == "python":
                        kernel_name, _ = kernel.prepare_kernel(
                            node_runtime, "r")
                        logger.info(
                            f"Notebook using kernel: {kernel_name} with environment {env_location}")
                    else:
                        raise Exception(
                            "Unknown language for this notebook")

                    PROCESS_SCRIPT = [
                        "papermill",
                        "--cwd", ".",
                        "--log-output --log-level DEBUG  --request-save-on-cell-execute",
                        "--autosave-cell-every 10",
                        "--progress-bar",
                        "-k", kernel_name,
                        " ".join(node_params),
                        node_filename,
                        output_notebook
                    ]

            elif node_group == 'r-node':
                logger.info('Detected node is R Script node')
                PROCESS_SCRIPT = [
                    "Rscript",
                    node_filename
                ]

            elif node_group == 'python-node':
                logger.info('Detected node is Python script node')
                PROCESS_SCRIPT = [
                    "python",
                    node_filename
                ]

            else:
                raise Exception(
                    "Invalid node group: notebook-node, r-node, python-node")

            PROCESS_SCRIPT = " ".join(PROCESS_SCRIPT)

            PROCESS_OUTPUT = "\n".join([
                f'path "{node_output[i]}",  emit: output{i+1}' for i in range(len(node_output))
            ])

            ENVIRONMENT = ""
            if len(node_envar) > 0:
                ENVIRONMENT = "\n".join(
                    [f"{e['key']}='{e['value']}'" for e in node_envar])

            LIMIT_MEMORY = ""
            if node_memory:
                LIMIT_MEMORY = f"memory '{node_memory}GB'"

            LIMIT_CPU = ""
            if node_cpu:
                LIMIT_CPU = f"cpus {node_cpu}"

            content = '''
            process PROCESS_NAME {
                tag { PROCESS_TAG }
                PROCESS_LABEL

                conda "PROCESS_CONDA_HOME_DIR"

                LIMIT_MEMORY
                LIMIT_CPU

                input:
                PROCESS_INPUT

                output:
                PROCESS_OUTPUT

                script:
                """
                ENVIRONMENT
                PROCESS_SCRIPT
                """
            }
            '''
            content = content.replace('PROCESS_NAME', process_name)
            content = content.replace('PROCESS_TAG', PROCESS_TAG)
            content = content.replace('PROCESS_LABEL', PROCESS_LABEL)
            content = content.replace('PROCESS_CONDA_HOME_DIR', node_runtime)
            content = content.replace('LIMIT_MEMORY', LIMIT_MEMORY)
            content = content.replace('LIMIT_CPU', LIMIT_CPU)
            content = content.replace('ENVIRONMENT', ENVIRONMENT)
            content = content.replace('PROCESS_INPUT', PROCESS_INPUT)
            content = content.replace('PROCESS_OUTPUT', PROCESS_OUTPUT)
            content = content.replace('PROCESS_SCRIPT', PROCESS_SCRIPT)

            for line in content.split('\n'):
                line = line.strip()
                print(line, file=f)

        # write workflow chanel
        node_chanel_nf = []
        upstream_node_list = []

        try:
            # array of node
            upstream_inputNodes = []
            try:
                upstream_inputNodes = node["inputs"][0]["links"]
            except:
                pass

            for upstream_node in upstream_inputNodes:
                print(upstream_node)
                if upstream_node['port_id_ref'] == 'outPort':
                    # find node in upstream_inputNodes
                    upstream_node = get_node_by_id(pipeline_data,
                                                   upstream_node["node_id_ref"])
                    if upstream_node is not None:
                        upstream_node_list.append(upstream_node)
                        # upstream_node.output  # array string

        except Exception as e:
            logger.info(f'Error when get upstream node {e}')

        logger.info(f"upstream_node_list {upstream_node_list}")

        for i in range(len(node_input)):
            node_input_file = node_input[i]
            # check node is step chanel or from previous step
            raw_chanel = True
            try:
                for upstream_node in upstream_node_list:
                    upstream_node_output_list = [
                        i for i in upstream_node["app_data"]["output"] if len(i.strip()) > 0]
                    for j in range(len(upstream_node_output_list)):
                        upstream_node_output = upstream_node_output_list[j]
                        if node_input_file == upstream_node_output:
                            logger.info(f'Match processing {node_input_file}')
                            # input file in from previous step
                            upstream_node_process_name = get_node_process_label(
                                upstream_node)
                            node_chanel_nf.append(
                                f'{node_name}_chanel_input{i+1}={upstream_node_process_name}.out.output{j+1}.collect()')

                            raw_chanel = False
                            break
            except Exception as e:
                logger.info(f'Error processing {e}')

            if raw_chanel:
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

    shutil.copy(params.input_file, params.output_dir)

    logger.info(f'Finished processing. Output: {params.output_dir}')

    return f'{params.output_dir}/main.nf'
