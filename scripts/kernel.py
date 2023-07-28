import json
import logging
import os
import subprocess

logger = logging.getLogger(__name__)


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
                        executable = data['argv'][0]
                    kernel_data.append(dict(
                        name=arr[0],
                        path=arr[1],
                        detail=data,
                        location=executable
                    ))
                except Exception as e:
                    logger.warning(f"Failed to load kernel data: {e}")
    except Exception as e:
        logger.info(f"Error while getting kernel list: {e}")

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
        except Exception as e:
            logger.info(f"Invalid environment path: {e}")
            raise Exception("Environment path is invalid")

        logger.info(
            f"Installing kernel: {kernel_type} with name {kernel_name}")

        if "python" in kernel_type:
            try:
                subprocess.check_output(
                    f'''mamba install -p {env_location} -c anaconda ipykernel''', shell=True)
                subprocess.check_output(
                    f'''/miniconda/user/bin/conda run -p {env_location} python -m ipykernel install --name "{kernel_name}" --display-name "{kernel_name}" --user''', shell=True)
            except Exception as e:
                logger.info(f"Error installing Python kernel: {e}")
                raise Exception("Failed to install the Python kernel")

        elif kernel_type == "r":
            try:
                subprocess.check_output(
                    f'''mamba install -p {env_location} -c conda-forge r-irkernel''', shell=True)
                subprocess.check_output(
                    f'''/miniconda/user/bin/conda run -p {env_location} Rscript -e "IRkernel::installspec(name='{kernel_name}', displayname='{kernel_name}', user=TRUE)"''', shell=True)
            except Exception as e:
                logger.info(f"Error installing R kernel: {e}")
                raise Exception("Failed to install the R kernel")
    else:
        raise Exception("Could not prepare this kernel")

    return kernel_name, env_location


def get_conda_env_path(env_name):
    try:
        conda_info = subprocess.check_output(['conda', 'info', '--json'])
        conda_info = json.loads(conda_info)
        envs_dir = conda_info['envs_dirs'][0]  # Get the default envs directory
        env_path = os.path.join(envs_dir, env_name)
        return env_path
    except Exception as e:
        logger.info(f"Error when get information environment: {e}")
        return None
