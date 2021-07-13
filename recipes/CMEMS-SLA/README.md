## Purpose

The purpose of the paired scripts `wget_cmems.py` and `wget_cmems.sh` is to provide an example of a traditional "download-model" workflow, to be contrasted with a workflow centered on analysis-ready, cloud-optimized (ARCO) data.

## Instructions

To run `wget_cmems.py`, first set env variables:
```
export CMEMS_USER=<username>
export CMEMS_PASSWORD=<password>
export CMEMS_DIRECTORY='my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047/dataset-duacs-rep-global-merged-allsat-phy-l4/'
```
> If you don't have an account with CMEMS, you can make one [here](https://resources.marine.copernicus.eu/?option=com_csw&task=results?option=com_csw&view=account).

Clear any prexisting files, if desired:
```
rm -r $CMEMS_DIRECTORY*
```

Make the shell script executable:
```
chmod +x wget_cmems.sh
```

Then run with:
```
python wget_cmems.py
```

Interrupting with `Ctrl+C` will print a message such as, e.g.:
```
Download interrupted after 2.31 minutes (138.3 seconds).
Of target range (1993-01-01 to 2017-05-15), 1993-01-16 was last day reached.
Downloaded 16 of 8901 files. (~0.93 GBs of ~517.03 GBs
Full download would take an additional ~21.33 hours (~1280.0 minutes).
```
