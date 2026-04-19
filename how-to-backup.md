# How to run the backup scripts

## First install the necessary software tools:

- Click [here](./how-to-install.md) to find out how to set up the necessary tools for running the backup scripts.

## To back up all institutions in one call:

- To back up all institutions in one call (with the csv for each institution
going into a file named <institution>.csv), simply run the following:

```
etl/do_backup.py
```

## To back up a specific institution:

- To back up one institution run the following:

```
etl/do_backup.py <institution> > output_file.csv

e.g., to back up each institution, one at a time, you could run the following:

etl/do_backup.py cali > cali_backup.csv
etl/do_backup.py dpla > dpla_backup.csv
etl/do_backup.py icaa > icaa_backup.csv
etl/do_backup.py mam > mam_backup.csv
etl/do_backup.py nhccnm > nhccnm_backup.csv
etl/do_backup.py nmma > nmma_backup.csv
etl/do_backup.py pth > pth_backup.csv
etl/do_backup.py si > si_backup.csv

```
