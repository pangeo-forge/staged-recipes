# There are many comments in this example. Remove them when you've
# finalized your pipeline.
import pandas as pd
import pangeo_forge
import pangeo_forge.utils
from pangeo_forge.tasks.http import download
from pangeo_forge.tasks.xarray import combine_and_write
from pangeo_forge.tasks.zarr import consolidate_metadata
from prefect import Flow, Parameter, task, unmapped

# We use Prefect to manage pipelines. In this pipeline we'll see
# * Tasks: https://docs.prefect.io/core/concepts/tasks.html
# * Flows: https://docs.prefect.io/core/concepts/flows.html
# * Parameters: https://docs.prefect.io/core/concepts/parameters.html

# A Task is one step in your pipeline. The `source_url` takes a day
# like '2020-01-01' and returns the URL of the raw data.


@task
def source_url(day: str) -> str:
    """
    Format the URL for a specific day.
    """
    day = pd.Timestamp(day)
    source_url_pattern = (
        "https://www.ncei.noaa.gov/data/"
        "sea-surface-temperature-optimum-interpolation/v2.1/access/avhrr/"
        "{day:%Y%m}/oisst-avhrr-v02r01.{day:%Y%m%d}.nc"
    )
    return source_url_pattern.format(day=day)


# All pipelines in pangeo-forge must inherit from pangeo_forge.AbstractPipeline


class Pipeline(pangeo_forge.AbstractPipeline):
    # You must define a few pieces of metadata in your pipeline.
    # name is the pipeline name, typically the name of the dataset.
    name = "example"
    # repo is the URL of the GitHub repository this will be stored at.
    repo = "pangeo-forge/example-pipeline"

    # Some pipelines take parameters. These are things like subsets of the
    # data to select or where to write the data.
    # See https://docs.prefect.io/core/concepts/parameters.htm for more
    days = Parameter(
        # All parameters have a "name" and should have a default value.
        "days",
        default=pd.date_range("1981-09-01", "1981-09-10", freq="D").strftime("%Y-%m-%d").tolist(),
    )
    cache_location = Parameter(
        "cache_location", default=f"gs://pangeo-forge-scratch/cache/{name}.zarr"
    )
    target_location = Parameter("target_location", default=f"gs://pangeo-forge-scratch/{name}.zarr")

    @property
    def sources(self):
        # This can be ignored for now.
        pass

    @property
    def targets(self):
        # This can be ignored for now.
        pass

    # The `Flow` definition is where you assemble your pipeline. We recommend using
    # Prefects Functional API: https://docs.prefect.io/core/concepts/flows.html#functional-api
    # Everything should happen in a `with Flow(...) as flow` block, and a `flow` should be returned.
    @property
    def flow(self):
        with Flow(self.name) as flow:
            # Use map the `source_url` task to each day. This returns a mapped output,
            # a list of string URLS. See
            # https://docs.prefect.io/core/concepts/mapping.html#prefect-approach
            # for more. We'll have one output URL per day.
            sources = source_url.map(self.days)

            # Map the `download` task (provided by prefect) to download the raw data
            # into a cache.
            # Mapped outputs (sources) can be fed straight into another Task.map call.
            # If an input is just a regular argument that's not a mapping, it must
            # be wrapepd in `prefect.unmapped`.
            # https://docs.prefect.io/core/concepts/mapping.html#unmapped-inputs
            # nc_sources will be a list of cached URLs, one per input day.
            nc_sources = download.map(sources, cache_location=unmapped(self.cache_location))

            # The individual files would be a bit too small for analysis. We'll use
            # pangeo_forge.utils.chunk to batch them up. We can pass mapped outputs
            # like nc_sources directly to `chunk`.
            chunked = pangeo_forge.utils.chunk(nc_sources, size=5)

            # Combine all the chunked inputs and write them to their final destination.
            writes = combine_and_write.map(
                chunked,
                unmapped(self.target_location),
                append_dim=unmapped("time"),
                concat_dim=unmapped("time"),
            )

            # Consolidate the metadata for the final dataset.
            consolidate_metadata(self.target_location, writes=writes)

        return flow


# pangeo-forge and Prefect require that a `flow` be present at the top-level
# of this module.
flow = Pipeline().flow
