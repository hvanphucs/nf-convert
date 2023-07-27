#!/usr/bin/env python3
import argparse
import json
import logging
import os
import re
import shutil
import subprocess
from distutils.dir_util import copy_tree

logger = logging.getLogger("validate_pipeline")


def configure_logging():
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    logger = logging.getLogger('validate_pipeline')
    logger.setLevel(logging.INFO)


def get_kernel_list():
    kernel_data = []
    try:
        out = subprocess.check_output('jupyter kernelspec list', shell=True)
        kernel_list = out.decode("utf-8")
        if "Available kernels" in kernel_list:
            kernel_list = [i for i in kernel_list.split("\n") if len(
                i.strip()) > 0 and "Available kernels:" not in i]

            for kernel in kernel_list:
                try:
                    arr = [i for i in kernel.split(" ") if len(i) > 0]
                    with open(f"{arr[1]}/kernel.json") as f:
                        data = json.load(f)
                        execuble = data['argv'][0]
                    kernel_data.append(dict(
                        name=arr[0],
                        path=arr[1],
                        detail=data,
                        location=execuble
                    ))
                except:
                    pass
    except:
        pass

    return kernel_data


def check_exist_kernel(env_location, kernel_type):
    r_kernel_exist = []
    py_kernel_exist = []

    if not os.path.exists(env_location):
        raise FileNotFoundError("The environment does not exist")

    kernel_list = get_kernel_list()
    for kernel in kernel_list:
        if env_location in kernel["location"]:
            if "lib/R/bin/R" in kernel["location"]:
                r_kernel_exist.append(kernel["name"])
            if "/bin/python" in kernel["location"]:
                py_kernel_exist.append(kernel["name"])

    if "python" in kernel_type:
        return py_kernel_exist
    elif kernel_type == "r":
        return r_kernel_exist
    else:
        raise Exception("The kernel is not yet supported")


def prepare_kernel(env_location, kernel_type):
    exist = check_exist_kernel(env_location, kernel_type)
    if len(exist) > 0:
        return exist[0], env_location

    if os.path.exists(env_location) and len(exist) == 0:
        try:
            kernel_name = env_location.split(
                "/.conda/envs/")[1] + f"_{kernel_type}"
        except:
            raise Exception("Enviroment path is invalid")

        logger.info(
            f"Installing kernel: {kernel_type} with name {kernel_name}")

        if "python" in kernel_type:
            procc1 = subprocess.check_output(
                f'''mamba install -p {env_location} -c anaconda ipykernel''', shell=True)
            procc2 = subprocess.check_output(
                f'''/miniconda/user/bin/conda run -p {env_location} python -m ipykernel install --name "{kernel_name}" --display-name "{kernel_name}" --user''', shell=True)

        elif kernel_type == "r":
            procc1 = subprocess.check_output(
                f'''mamba install -p {env_location} -c conda-forge r-irkernel''', shell=True)
            procc2 = subprocess.check_output(
                f'''/miniconda/user/bin/conda run -p {env_location} Rscript -e "IRkernel::installspec(name='{kernel_name}', displayname='{kernel_name}', user=TRUE)"''', shell=True)
    else:
        raise Exception("Could prepare this kernel")

    return kernel_name, env_location


def to_camel_case(input_str):
    valid_chars = re.sub(r'[^a-zA-Z0-9]', '', input_str)
    lower_case_str = valid_chars.lower()
    words = lower_case_str.split('_')
    camel_case_words = [words[0]] + [word.capitalize() for word in words[1:]]
    camel_case_str = ''.join(camel_case_words)
    return camel_case_str


def read_params():
    parser = argparse.ArgumentParser(
        description='Convert the Elyra pipeline-style JSON to a Nextflow pipeline')
    parser.add_argument('-i', '--input-file', dest='input_file', required=True,
                        help='Output directory where Nextflow pipeline is written')
    parser.add_argument('-f', '--force-create', dest='force_create', action='store_true',
                        help='Overwrite existing output directory')
    parser.add_argument('-o', '--output-dir', dest='output_dir', required=True,
                        help='Output directory where Nextflow pipeline is written')

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


def format_print_node(node):
    ignoreKey = ["ui_data"]
    beautyNode = {}
    for key, value in node.items():
        if not key in ignoreKey:
            beautyNode[key] = value
    return beautyNode


def get_node_by_id(node_list, id):
    for node in node_list:
        if node.id == id:
            return node
    return None


def main():
    configure_logging()

    params = read_params()

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
        node_type = node.get('type', None)  # execution_node
        node_group = node.get('op', None)  # notebook-node

        logger.info("\n----Process node-----")
        logger.info(format_print_node(node))

        if node_type != 'execution_node' or node_group is None:
            logger.warning(
                f"Ignore this node because node_type={node_type} and node_group={node_group}")
            continue

        node_params = node.get('app_data', {})

        node_label = node_params.get('label', None)
        node_filename = node_params.get('filename', None)
        node_runtime = node_params.get('runtime_environment', None)
        node_cpu = node_params.get('cpu', None)
        node_memory = node_params.get('memory', 4)
        node_input = node_params.get('dependencies', [])
        node_output = node_params.get('output', [])
        node_envar = node_params.get('env_vars', [])

        if node_filename is not None:
            node_filename = get_full_path(node_filename)

        if node_label == None or len(node_label.strip()) == 0:
            basename = os.path.basename(node_filename).split(".")[0]
            node_label = f"{basename}" + "_" + node_id[0:3]
        else:
            node_label = node_label + "_" + node_id[0:3]

        node_name = to_camel_case(node_label)

        process_name = node_name.upper()
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
                            kernel_name, env_location = prepare_kernel(
                                node_runtime, "python")
                            logger.info(
                                f"Notebook using kernel: {kernel_name} with environment {env_location}")
                        elif language == "python":
                            kernel_name, _ = prepare_kernel(
                                node_runtime, "r")
                            logger.info(
                                f"Notebook using kernel: {kernel_name} with environment {env_location}")
                        else:
                            fnotebook.close()
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
                f'path "{node_output[i]}"' for i in range(len(node_output))
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

            upstream_inputNodes = node["inputs"][0]["links"]
            for upstream_node in upstream_inputNodes:
                if upstream_node['port_id_ref'] == 'outPort':
                    # find node in upstream_inputNodes
                    upstream_node = get_node_by_id(
                        upstream_node["node_id_ref"])
                    if upstream_node['port_id_ref'] is not None:
                        upstream_node_list.append(upstream_node)

        except Exception as e:
            logging.info('Error when get upstream node', e)

        logging.info("upstream_node_list", upstream_node_list)

        for i in range(len(node_input)):
            node_input_file = node_input[i]
            # check node is step chanel or from previous step

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


if __name__ == '__main__':
    main()
