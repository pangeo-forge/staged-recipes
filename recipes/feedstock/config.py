# vi local-runner-config.py
c.Bake.bakery_class = 'pangeo_forge_runner.bakery.local.LocalDirectBakery'
c.Bake.prune = True
c.LocalDirectBakery.num_workers = 1

c.MetadataCacheStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# Metadata cache should be per `{{job_name}}`, as kwargs changing can change metadata
c.MetadataCacheStorage.root_path = 'local_storage/metadatacache/'

c.TargetStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
c.TargetStorage.root_path = 'local_storage/target/'

# c.InputCacheStorage.fsspec_class = 'fsspec.implementations.local.LocalFileSystem'
# c.InputCacheStorage.root_path = 'local_storage/cache/'
