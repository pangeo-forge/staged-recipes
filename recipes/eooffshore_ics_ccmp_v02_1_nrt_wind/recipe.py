import pandas as pd
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def ics_wind_speed_direction(ds, fname):
    """
    Selects a subset for the Irish Continental Shelf (ICS) region, and computes wind speed and
    direction for the u and v components in the specified product. Dask arrays are
    created for delayed execution.
    """
    import dask
    import dask.array as da
    from datetime import datetime
    from metpy.calc import wind_direction, wind_speed
    import xarray as xr

    @dask.delayed
    def delayed_metpy_fn(fn, u, v):
        return fn(u, v).values

    # ICS grid
    geospatial_lat_min = 45.75
    geospatial_lat_max = 58.25
    geospatial_lon_min = 333.85
    geospatial_lon_max = 355.35
    icds = ds.sel(
        latitude=slice(geospatial_lat_min, geospatial_lat_max),
        longitude=slice(geospatial_lon_min, geospatial_lon_max),
    )

    # Remove subset of original attrs as they're no longer relevant
    for attr in ["base_date", "date_created", "history"]:
        del icds.attrs[attr]

    # Update the grid attributes
    icds.attrs.update(
        {
            "geospatial_lat_min": geospatial_lat_min,
            "geospatial_lat_max": geospatial_lat_max,
            "geospatial_lon_min": geospatial_lon_min,
            "geospatial_lon_max": geospatial_lon_max,
        }
    )
    u = icds.uwnd
    v = icds.vwnd
    # Original wind speed 'units': 'm s-1' attribute not accepted by MetPy,
    # use the unit contained in ERA5 data
    ccmp_wind_speed_units = u.units
    era5_wind_speed_units = "m s**-1"
    u.attrs["units"] = era5_wind_speed_units
    v.attrs["units"] = era5_wind_speed_units

    variables = [
        {
            "name": "wind_speed",
            "metpy_fn": wind_speed,
            "attrs": {"long_name": "Wind speed", "units": ccmp_wind_speed_units},
        },
        {
            "name": "wind_direction",
            "metpy_fn": wind_direction,
            "attrs": {"long_name": "Wind direction", "units": "degree"},
        },
    ]

    # CCMP provides u/v at a single height, 10m
    for variable in variables:
        icds[variable["name"]] = (
            xr.DataArray(
                da.from_delayed(
                    delayed_metpy_fn(variable["metpy_fn"], u, v), u.shape, dtype=u.dtype
                ),
                coords=u.coords,
                dims=u.dims,
            )
            .assign_coords(height=10)
            .expand_dims(["height"])
        )
        icds[variable["name"]].attrs.update(variable["attrs"])

    icds.height.attrs.update(
        {
            "long_name": "Height above the surface",
            "standard_name": "height",
            "units": "m",
        }
    )
    # Restore units
    for variable in ["uwnd", "vwnd"]:
        icds[variable].attrs["units"] = ccmp_wind_speed_units

    icds.attrs["eooffshore_zarr_creation_time"] = datetime.strftime(
        datetime.now(), "%Y-%m-%dT%H:%M:%SZ"
    )
    icds.attrs[
        "eooffshore_zarr_details"
    ] = "EOOffshore Project: Concatenated CCMP v0.2.1.NRT 6-hourly wind products provided by Remote Sensing Systems (RSS), for Irish Continental Shelf. Wind speed and direction have been calculated from the uwnd and vwnd variables. CCMP Version-2 vector wind analyses are produced by Remote Sensing Systems. Data are available at www.remss.com."
    return icds


missing_dates = [
    pd.Timestamp(d)
    for d in [
        "2017-03-11",
        "2017-03-12",
        "2017-04-10",
        "2017-05-25",
        "2017-05-26",
        "2018-01-04",
        "2020-07-09",
        "2020-07-13",
        "2020-07-15",
        "2020-08-04",
        "2020-08-09",
        "2020-08-11",
        "2020-10-22",
        "2020-10-23",
    ]
]

dates = pd.date_range("2015-01-16", "2021-09-30", freq="D")

# Drop missing dates
dates = dates.drop(missing_dates)


def make_url(time):
    year = time.strftime("%Y")
    month = time.strftime("%m")
    day = time.strftime("%d")
    url = f"https://data.remss.com/ccmp/v02.1.NRT/Y{year}/M{month}/CCMP_RT_Wind_Analysis_{year}{month}{day}_V02.1_L3.0_RSS.nc"
    return url


# Daily products with 6-hourly values
NITEMS_PER_FILE = 4

pattern = FilePattern(
    make_url,
    ConcatDim(name="time", keys=dates, nitems_per_file=NITEMS_PER_FILE),
)

target_chunks = {"time": 8000, "latitude": -1, "longitude": -1}

recipe = XarrayZarrRecipe(
    file_pattern=pattern,
    target_chunks=target_chunks,
    inputs_per_chunk=int(target_chunks["time"] / NITEMS_PER_FILE),
    process_input=ics_wind_speed_direction,
)
