#!/usr/bin/env python3
import json
import os


def update_db(run_config, db_file):
    current_db = []
    existOne = False

    parent = os.path.dirname(db_file)
    if not os.path.exists(parent):
        os.makedirs(parent)

    try:
        with open(db_file, "r") as f:
            json.load(f)

        for run in current_db:
            if run["run_id"] == run_config["run_id"]:
                existOne = True
                break
    except:
        pass

    if not existOne:
        current_db.append(run_config)
    with open(db_file, "r") as f:
        json.dumps(current_db, f, indent=4)
