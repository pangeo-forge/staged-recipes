from dataclasses import dataclass
from datetime import datetime
import itertools
import operator
import pandas as pd
from pangeo_forge_recipes.executors.base import Pipeline, Stage
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import setup_logging, XarrayZarrRecipe
from pangeo_forge_recipes.recipes.xarray_zarr import open_input
from typing import Dict, List
import xarray as xr


def process_ds(
    ds: xr.Dataset, fname: str, satellites: List[str], service: str
) -> xr.Dataset:
    """
    Selects the Irish Continental Shelf coordinates, and replaces the redundant
    'time' values with a unique dataset measurement time.
    A height dimension is also added for the wind speed and direction variables.
    """
    # Select Irish Continental Shelf (ICS) coordinates
    lon_west = 334
    lon_east = 360
    lat_south = 46
    lat_north = 58.1
    ds = ds.sel(lat=slice(lat_south, lat_north), lon=slice(lon_west, lon_east))

    # Replace original time dimension with corresponding 'measurement_time'"""
    time_attrs = ds.measurement_time.attrs
    time_attrs[
        "long_name"
    ] = f'{time_attrs["long_name"]} (mean over lat/lon dimensions)'
    # 'measurement_time' isn't uniform for all lat/lon coordinates at a particular time
    # The spatial mean time is used - the range is ~1.75 hours for the ICS subset coordinates
    measurement_time = ds.measurement_time.mean(dim=["lat", "lon"])
    # Occasionally, certain measurement_times and corresponding variable values will only contain NaT/NaN
    # for a particular time
    # Drop these now

    # Load data to avoid following occasional error:
    # "IndexError: The indexing operation you are attempting to perform is not valid on netCDF4.Variable object. Try loading your data into memory first by calling .load()."
    ds = ds.load().where(measurement_time.notnull(), drop=True)
    # Replace original time coordinates
    ds = (
        ds.assign_coords(measurement_time=measurement_time)
        .drop("time")
        .rename({"measurement_time": "time"})
    )
    ds.time.attrs = time_attrs

    # https://github.com/pangeo-forge/staged-recipes/pull/119
    # Ensure valid encoding is used
    ds.time.encoding["calendar"] = "proleptic_gregorian"
    # From source measurement_time variable
    ds.time.encoding["units"] = "seconds since 1990-01-01 00:00:00"

    # Add height dimension for 10m wind data
    data_height = 10
    ds["wind_speed"] = ds.wind_speed.assign_coords(height=data_height).expand_dims(
        ["height"]
    )
    ds["wind_to_dir"] = ds.wind_to_dir.assign_coords(height=data_height).expand_dims(
        ["height"]
    )
    ds.height.attrs.update(
        {
            "long_name": "Height above the surface",
            "standard_name": "height",
            "units": "m",
        }
    )

    service_dois = {
        "WIND_GLO_WIND_L3_NRT_OBSERVATIONS_012_002": "https://doi.org/10.48670/moi-00182",
        "WIND_GLO_WIND_L3_REP_OBSERVATIONS_012_005": "https://doi.org/10.48670/moi-00183",
    }

    ds.attrs["eooffshore_zarr_creation_time"] = datetime.strftime(
        datetime.now(), "%Y-%m-%dT%H:%M:%SZ"
    )
    ds.attrs[
        "eooffshore_zarr_details"
    ] = f"EOOffshore Project: Concatenated Copernicus Marine Service {service} Metop-{'/'.join(satellites)} ASCAT Ascending/Descending products, for the Irish Continental Shelf. Original products time coordinates have been replaced with the satellite/pass measurement_time values. Generated using E.U. Copernicus Marine Service Information; {service_dois[service]}"
    return ds


@dataclass
class AscatXarrayZarrRecipe(XarrayZarrRecipe):
    satellite_dates: Dict[str, Dict[str, str]] = None
    satellite_passes: List[str] = None
    host: str = None
    service: str = None
    missing_products: List[str] = None

    def __post_init__(self):
        self.target_chunks = {"height": 1, "time": 1500, "lat": -1, "lon": -1}
        self.inputs_per_chunk = self.target_chunks["time"]
        # This ensures that both netcdf3 (older Metop-A/B files) and netcdf4 (Metop-C) are supported
        # Must be accompanied by 'unknown' file_type above
        self.xarray_open_kwargs = {"engine": "netcdf4"}

        if not self.file_pattern:
            # If not specified, create the file pattern based on combinations of satellite/date/pass
            def missing_product(
                satellite: str, date: pd.Timestamp, satellite_pass: str
            ) -> bool:
                return (
                    f"GLO-WIND_L3-OBS_METOP-{satellite}_ASCAT_12_{satellite_pass}_{date.strftime('%Y%m%d')}.nc"
                    in set(self.missing_products)
                )

            file_parameters = itertools.chain(
                *[
                    itertools.product(
                        [satellite],
                        pd.date_range(dates["start"], dates["end"], freq="D"),
                        self.satellite_passes,
                    )
                    for satellite, dates in self.satellite_dates.items()
                ]
            )
            # Sort by date, this will simplify pruned recipe testing
            file_parameters = sorted(file_parameters, key=operator.itemgetter(1))
            file_list = [
                self._make_url(satellite, date, satellite_pass)
                for satellite, date, satellite_pass in file_parameters
                if not missing_product(satellite, date, satellite_pass)
            ]
            self._create_file_pattern(file_list=file_list)

        self.process_input = lambda ds, fname: process_ds(
            ds=ds,
            fname=fname,
            satellites=self.satellite_dates.keys(),
            service=self.service,
        )
        super().__post_init__()

        # Insert additional stage into pipeline that sorts the
        # ASCAT pattern products according to measurement_time (spatial mean)
        self._original_stages = self._compiler().stages
        self._compiler = self._ascat_xarray_zarr_recipe_compiler

    def _ascat_xarray_zarr_recipe_compiler(self) -> Pipeline:
        """Custom pipeline compiler containing additional sorting stage"""
        stages = list(self._original_stages)
        stages.insert(
            1, Stage(name="sort_file_pattern", function=self._sort_file_pattern)
        )
        return Pipeline(stages=stages, config=self)

    def _create_file_pattern(self, file_list: List[str]) -> None:
        """Creates the recipe file pattern from the specified list"""
        self.file_pattern = pattern_from_file_sequence(
            file_list=file_list,
            concat_dim="time",
            nitems_per_file=1,
            # Must be set to unknown, as default type of 'netcdf4' will prevent xarray engine from being specified
            file_type="unknown",
            fsspec_open_kwargs={
                "username": "rabernathey",
                "password": "oCg!25s%DN^M",
                "block_size": 400_000_000,
            },
        )

    def _sort_file_pattern(self, *, config: XarrayZarrRecipe) -> None:
        """Replace the recipe file pattern with new pattern containing time-sorted file URLs"""
        file_times = []
        for input_key, input_filename in self.file_pattern.items():
            with open_input(input_key=input_key, config=self) as ds:
                # Exclude files with NaT measurement_time in the source data product
                if ds.dims["time"] == 1:
                    file_times.append((ds.time.values[0], input_filename))
        # New pattern with files sorted according to (measurement_)time
        newlist = [input_filename for _, input_filename in sorted(file_times)]
        self._create_file_pattern(
            file_list=[input_filename for _, input_filename in sorted(file_times)]
        )

    def _make_url(self, satellite: str, date: pd.Timestamp, satellite_pass: str) -> str:
        """Creates an ASCAT product FTP URL"""
        year = date.strftime("%Y")
        month = date.strftime("%m")
        day = date.strftime("%d")
        nrt = self.host.startswith("nrt")
        return f"ftp://{self.host}/Core/{self.service}/KNMI-GLO-WIND_L3-{'REP-' if not nrt else ''}OBS_METOP-{satellite}_ASCAT_12_{satellite_pass}{'_V2' if nrt else ''}/{year}/{month}/GLO-WIND_L3-OBS_METOP-{satellite}_ASCAT_12_{satellite_pass}_{year}{month}{day}.nc"


setup_logging()

nrt_recipe = AscatXarrayZarrRecipe(
    file_pattern=None,
    satellite_dates={
        "A": {"start": "2016-01-01", "end": "2021-09-30"},
        "B": {"start": "2016-01-01", "end": "2021-09-30"},
        "C": {"start": "2019-10-28", "end": "2021-09-30"},
    },
    satellite_passes=["ASC", "DES"],
    host="nrt.cmems-du.eu",
    service="WIND_GLO_WIND_L3_NRT_OBSERVATIONS_012_002",
    missing_products=[
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20170528.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190731.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190801.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190802.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190803.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190804.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20170528.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190731.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190801.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190802.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190803.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190804.nc",
    ],
)

rep_recipe = AscatXarrayZarrRecipe(
    file_pattern=None,
    satellite_dates={
        "A": {"start": "2007-01-01", "end": "2021-07-31"},
        "B": {"start": "2019-01-01", "end": "2021-07-31"},
    },
    satellite_passes=["ASC", "DES"],
    host="my.cmems-du.eu",
    service="WIND_GLO_WIND_L3_REP_OBSERVATIONS_012_005",
    missing_products=[
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20070228.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20070421.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20070422.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20070423.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20070424.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20070918.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20080117.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20080320.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20110515.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20170528.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190731.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190801.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190802.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190803.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_ASC_20190804.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20070228.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20070421.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20070422.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20070423.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20070424.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20070918.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20080117.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20080320.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20110515.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20170528.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190731.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190801.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190802.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190803.nc",
        "GLO-WIND_L3-OBS_METOP-A_ASCAT_12_DES_20190804.nc",
    ],
)
