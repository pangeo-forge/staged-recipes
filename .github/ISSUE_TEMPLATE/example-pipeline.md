---
name: Proposed Recipe
about: This template helps us gather information about possible use cases for pangeo-forge
title: Example pipeline for [Dataset Name]
labels: example
assignees: ''

---

<!--
This template is to describe a potential pipeline for Pangeo Forge to create analysis-ready, cloud-optimized data from an upstream data repository.

A pipeline has three basic stages:
1. Download the source files from the upstream repository in whatever format they are stored.
2. Perform any transformations that are needed in order to make the data "analysis ready."
3. Write out a new dataset in a cloud optimized format
-->

## Source Dataset

<!-- Describe your dataset in a few sentences below. -->

<!-- Please also provide the following information by editing the list below. -->

- Link to the website / online documentation for the data
- The file format (e.g. netCDF, csv)
- How are the source files organized? (e.g. one file per day)
- How are the source files accessed (e.g. FTP)
  - provide an example link if possible
- Any special steps required to access the data (e.g. password required)

## Transformation / Alignment / Merging

<!--
Describe below how the files should be combined into one analysis-ready dataset.
For example, "the files should be concatenated along the time dimension."
Are there any other transformations or checks that should be performed to make the data more "analysis ready"?
-->


## Output Dataset

<!--
How do you want the output of the pipeline to be stored?
Cloud optimized formats such as zarr, tiledb, or parquet are recommended.
If possible, provide details on how you would like the output to be structured
(e.g. number of different output datasets, chunk / partition size, etc.)
-->
