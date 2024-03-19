# vi local-runner-config.py
c.Bake.bakery_class = 'pangeo_forge_runner.bakery.local.LocalDirectBakery'
c.Bake.prune = False
c.LocalDirectBakery.num_workers = 4

c.MetadataCacheStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# Metadata cache should be per `{{job_name}}`, as kwargs changing can change metadata
c.MetadataCacheStorage.root_path = 'local_storage/metadatacache/'

c.TargetStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
c.TargetStorage.root_path = 'local_storage/target/'


# c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
# c.TargetStorage.root_path = f"s3://carbonplan-scratch/pyramid_test_consolidated"

# c.InputCacheStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# c.InputCacheStorage.root_path = 'local_storage/cache/'


# c.Bake.prune = 0
# c.Bake.bakery_class = 'pangeo_forge_runner.bakery.local.LocalDirectBakery'
# c.LocalDirectBakery.num_workers = 5
# BUCKET_PREFIX = "s3://pangeo-forge-veda-output"
# c.TargetStorage.fsspec_class = "s3fs.S3FileSystem"
# #BUCKET_PREFIX = "/home/jovyan/"
# #c.TargetStorage.fsspec_class = "fsspec.implementations.local.LocalFileSystem"
# c.TargetStorage.root_path = f"{BUCKET_PREFIX}/{{job_name}}/output"
# c.TargetStorage.fsspec_args = {
#   "anon": False,
#   "client_kwargs": {"region_name": "us-west-2"}
# }
# #c.InputCacheStorage.fsspec_class = c.TargetStorage.fsspec_class
# #c.InputCacheStorage.fsspec_args = c.TargetStorage.fsspec_args
# #c.InputCacheStorage.root_path = f"{BUCKET_PREFIX}/cache/"

