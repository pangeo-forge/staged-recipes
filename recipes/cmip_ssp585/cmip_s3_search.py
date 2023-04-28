import pandas as pd
import s3fs

class CMIPS3Search():

    def __init__(self, substrings, variables):
        self.catalog = self._init_catalog()
        self.substrs = substrings
        self.vars = variables 
        self.df = self._init_matching_df()

    def _init_catalog(self):
        netcdf_cat = 's3://cmip6-nc/esgf-world.csv.gz'
        df_s3 = pd.read_csv(netcdf_cat, dtype='unicode')
        df_s3['dataset'] = (
            df_s3.apply(lambda row: '.'.join(row.path.split('/')[6:12]),axis=1)
        )
        return df_s3

    def _match_substring(self):
        """
        """
        match_list = [
            self.catalog[self.catalog.dataset.str.contains(self.substrs[i])] 
            for i in range(len(self.substrs))
        ]
        return pd.concat(match_list)

    def _drop_superstr_vars(self):
        """
        """
        vars_to_drop = [
            v for v in self.df["variable"].unique() if v not in self.vars
        ]
        for v in vars_to_drop:
            self.df = self.df[self.df.variable != v]

    def _init_tuples(self):
        self.tuples = self.df["dataset"].unique()

    def _prune_to_latest_versions(self):
        """
        """
        latest_versions = []
        for t in self.tuples:
            df = self.df[self.df["dataset"] == t]
            last_version = sorted(df.version.unique())[-1]
            df2 = df[df.version == last_version].reset_index(drop=True)
            print(f"{t}: {len(df)} -> {len(df2)}")
            latest_versions.append(df2)
        return pd.concat(latest_versions)

    def _init_matching_df(self):
        """
        """
        self.df = self._match_substring()
        self._drop_superstr_vars()
        self._init_tuples()
        return self._prune_to_latest_versions()

    @staticmethod
    def _get_url_size(fname):
        fs_s3 = s3fs.S3FileSystem(anon=True)

        with fs_s3.open(fname, mode="rb") as of:
            size = of.size
        return size

    def print_sizes(self):
        """
        """
        total_size = 0
        for t in self.tuples:
            df = self.df[self.df["dataset"] == t]
            tuple_ds_size = 0
            for p in df.path:
                tuple_ds_size += self._get_url_size(p)/1e9
            print(f"{t}: {round(tuple_ds_size, 2)} GB, {len(df.path)} source files.")
            total_size += tuple_ds_size
        print(f"total_size: {round(total_size, 2)}")

    def return_inputs(self):
        """
        """
        inputs = {}
        for t in self.tuples:
            inputs.update(
                {t: list(self.df[self.df["dataset"] == t].path)}
            )
        return inputs

