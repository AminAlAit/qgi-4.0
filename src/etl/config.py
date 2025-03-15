"""TODO"""

import os
import yaml


def load_config(config_path = "config.yaml"):
    # Get the directory of the current script (app.py)
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path to config.yaml
    config_full_path = os.path.join(current_dir, config_path)

    with open(config_full_path, "r") as f:
        config = yaml.safe_load(f)
    return config
