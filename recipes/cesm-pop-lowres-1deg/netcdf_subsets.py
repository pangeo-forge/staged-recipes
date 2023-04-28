import os
import time

import numpy as np
import xarray as xr


class NetCDFSubsets:
    def __init__(
        self,
        cache_fs,
        cache_dir,
        var_name,
        target_bins,
        concat_dim_name,
        concat_dim_length,
    ):
        self.cache_fs = cache_fs
        self.cache_dir = cache_dir
        self.var_name = var_name
        self.target_bins = target_bins
        self.concat_dim_name = concat_dim_name
        self.concat_dim_length = concat_dim_length

    def _fn_from_var(self):
        """Assumes one netCDF per variable in cache"""
        for filename in self.cache_fs.ls(self.cache_dir):
            if f'{self.var_name.lower()}.' in filename:
                print(f'Filename for {self.var_name} is {filename}')
                return filename

    def _open_dataset(self):
        fn = self._fn_from_var()
        open_file = self.cache_fs.open(fn)
        print(f'Calling `xr.open_dataset` on {open_file}')
        start = time.time()
        ds = xr.open_dataset(open_file)
        print(f'Opened dataset in {time.time()-start:.02f}s')
        assert len(ds[self.concat_dim_name]) == self.concat_dim_length
        print(f"`len(ds['{self.concat_dim_name}']`) matches expected length")
        return ds

    def _assign_time_counter(self):
        ds = self._open_dataset()
        array = np.arange(1, self.concat_dim_length + 1, 1)
        return ds.assign_coords(time_counter=(self.concat_dim_name, array))

    def _grouby_bins(self):
        ds = self._assign_time_counter()
        groupby = ds.groupby_bins('time_counter', self.target_bins)
        bins, datasets = zip(*groupby)
        return bins, datasets

    def _make_target_paths(self, bins):
        def format_filename(interval_object, counter, variable):
            out = str(interval_object).replace('(', '')
            if '-' in out:  # only relevant for the first bin
                out = out.replace('-', '')
            out = out.replace(']', '')
            out = out.replace(', ', '-')
            return f'{variable}-{counter}-{out}.nc'

        return [format_filename(b, i, self.var_name) for i, b in enumerate(bins)]

    def subset_netcdf(self):

        bins, datasets = self._grouby_bins()
        paths = self._make_target_paths(bins=bins)

        start = time.time()
        for i, p in enumerate(paths):

            loop_start = time.time()

            print(f'Writing {p} to local')
            datasets[i].to_netcdf(p)

            print(f'Uploading {p} to {self.cache_dir}/subsets/{p}')
            self.cache_fs.put(p, f'{self.cache_dir}/subsets/{p}')

            print(f'Removing {p} from local')
            os.remove(p)

            print(
                f'Total elapsed: {(time.time()-start):.2f}s \n'
                f'This iteration: {(time.time()-loop_start):.2f}s'
            )
        print('`subset_netcdf` complete')
