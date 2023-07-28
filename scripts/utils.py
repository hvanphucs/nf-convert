import json
import os
from datetime import datetime


def get_full_path(path):
    if not path.startswith('/'):
        return os.path.expanduser('~') + "/" + path
    return path


def to_camel_case(input_str):
    valid_chars = re.sub(r'[^a-zA-Z0-9]', '', input_str)
    lower_case_str = valid_chars.lower()
    words = lower_case_str.split('_')
    camel_case_words = [words[0]] + [word.capitalize() for word in words[1:]]
    camel_case_str = ''.join(camel_case_words)
    return camel_case_str


def write_to_checkpoint(params, data):
    checkpoint_dir = f"{params.output_dir}"
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir)

    checkpoint_file = os.path.join(checkpoint_dir, "run.json")

    with open(checkpoint_file, 'w') as f:
        json.dump(data, f, indent=4)


def now():
    return datetime.now().strftime("%d/%m/%Y %H:%M:%S")
