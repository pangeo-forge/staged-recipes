from fsspec.implementations.local import LocalFileSystem
from collections import defaultdict

# Let's put all our data on the same dir as this config file
from pathlib import Path
import os
PWD = Path(__file__).parent

DATA_PREFIX = os.path.join(PWD, 'data')
os.makedirs(DATA_PREFIX, exist_ok=True)

# provided via compile() / exec()
c = get_config()
c.Bake.prune = True

c.TargetStorage.root_path = f"{DATA_PREFIX}/{{job_name}}"
c.TargetStorage.fsspec_class = f"{LocalFileSystem.__module__}.{LocalFileSystem.__name__}"

c.InputCacheStorage.root_path = f"{DATA_PREFIX}/cache/input"
c.InputCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
c.InputCacheStorage.fsspec_args = c.TargetStorage.fsspec_args

c.MetadataCacheStorage.root_path = f"{DATA_PREFIX}/{{job_name}}/cache/metadata"
c.MetadataCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
c.MetadataCacheStorage.fsspec_args = c.TargetStorage.fsspec_args