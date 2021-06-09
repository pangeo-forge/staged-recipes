import s3fs
import xarray as xr

fs_s3 = s3fs.S3FileSystem(anon=True)


def target_chunks(input_url):
    """
    """
    file_url = fs_s3.open(input_url, mode='rb')
    ds = xr.open_dataset(file_url)

    ntime = len(ds.time)       # the number of time slices
    chunksize_optimal = 50e6  # desired chunk size in bytes
    ncfile_size = ds.nbytes    # the netcdf file size
    chunksize = max(int(ntime* chunksize_optimal/ ncfile_size),1)

    target_chunks = ds.dims.mapping
    target_chunks['time'] = chunksize

    return target_chunks