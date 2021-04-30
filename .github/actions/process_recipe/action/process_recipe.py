import sys
import os
import json
from pangeo_forge_prefect.flow_manager import register_flow

def main():
    secrets = json.loads(os.environ["SECRETS_CONTEXT"])
    print(secrets)
    meta_yaml = os.environ["INPUT_PATH_TO_META_YAML"]
    bakeries_yaml = os.environ["INPUT_PATH_TO_BAKERIES_YAML"]
    register_flow(meta_yaml, bakeries_yaml, secrets)

if __name__ == "__main__":
    main()
