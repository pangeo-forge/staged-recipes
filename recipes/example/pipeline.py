from pangeo_forge.pipelines.base import AbstractPipeline
from pangeo_forge.tasks.http import download
from prefect import Flow, task
from prefect.environments import DaskKubernetesEnvironment
from prefect.environments.storage import Docker

name = "HadCRUT4"
cache_location = f"gs://pangeo-scratch/{name}-cache/"
target_location = f"gs://pangeo-forge/{name}/4.6.0.0"


@task
def write(source_url, target_url):
    fs = fsspec.get_filesystem_class(source_url.split(":")[0])(token="cloud")
    ds = xr.open_dataset(fs.open(source_url))
    mapper = fs.get_mapper(target_url)
    ds.to_zarr(mapper)
    return target_url


class HadCRUT4Pipeline(AbstractPipeline):
    def __init__(self, cache_location, target_location):
        self.cache_location = cache_location
        self.target_location = target_location

    @property
    def environment(self):
        """Note: this property will eventually be moved upstream"""
        environment = DaskKubernetesEnvironment(
            min_workers=1,
            max_workers=30,
            scheduler_spec_file="recipe/job.yaml",
            worker_spec_file="recipe/worker_pod.yaml",
        )
        return environment

    @property
    def storage(self):
        """Note: this property will eventually be moved upstream"""
        storage = Docker(
            "pangeoforge",
            dockerfile="recipe/Dockerfile",
            prefect_directory="/home/jovyan/prefect",
            python_dependencies=[
                "git+https://github.com/pangeo-forge/pangeo-forge@master",
                "prefect==0.13.4",
            ],
            image_tag="latest",
        )
        return

    @property
    def sources(self):
        return [
            "https://crudata.uea.ac.uk/cru/data/temperature/HadCRUT.4.6.0.0.median.nc"
        ]

    @property
    def targets(self):
        return [self.target_location]

    @property
    def flow(self):
        with Flow(
            self.name, storage=self.storage, environment=self.environment
        ) as _flow:

            nc_source = download(self.sources[0], self.cach_location)
            write(nc_source, self.target_url)

        return _flow


pipeline = HadCRUT4Pipeline(cache_location, target_location)

if __name__ == "__main__":
    pipeline.flow.validate()

    print(pipeline.flow)
    print(pipeline.flow.environment)
    print(pipeline.flow.parameters)
    print(pipeline.flow.sorted_tasks())
    print("Registering Flow")
    pipeline.flow.register(project_name="pangeo-forge")
