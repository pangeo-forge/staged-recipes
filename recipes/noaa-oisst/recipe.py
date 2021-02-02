import pandas as pd

dates = pd.date_range("1981-09-01", "2021-01-05", freq="D")
input_urls = [
    input_url_pattern.format(
        yyyymm=day.strftime("%Y%m"), yyyymmdd=day.strftime("%Y%m%d")
    )
    for day in dates
]

recipe = NetCDFtoZarrSequentialRecipe(
    input_urls=input_urls,
    sequence_dim="time",
    inputs_per_chunk=20
)
