# Data Processing for ProgressPHL Dashboard

## The main data processing script

The main data processing script:

```bash
poetry run progressphl-data etl --version 2
```

You can specify the SPI version to use with the `--version` flag. The default is `2`.
The version corresponds to the input data files in `progressphl_data/_cache/`.

### Inputs

The main input to the ETL script is the SPI data file. The default is `progressphl_data/_cache/ProgressPHL_Recalculated_v1.xlsx`. This is the excel spreadsheet of SPI data received from 
the SPI team. The file is loaded in the `get_spi_data()` function in `core.py`.

If you want to add a new version of the data, you should create a new folder in 
the `_cache/` folder with the version number (e.g., `v3`), and place the new
excel spreadsheet in the folder. Then you should add a new piece of code to the 
`get_spi_data()` function in `core.py` to load the new data. The new code
should load both the SPI values and the indicator values.

### Outputs

The processed data is saved in the `data-products/dashboard-inputs/` folder. 
These files are also uploaded to *public* s3 bucket on the Controller's Office AWS
account. The production dashboard reads in the data from this s3 bucket.

The output data files are:


## Development set up

1. Clone this repository.
2. Install the package with poetry. In the main repo folder:

```bash
poetry install
```

3. Run the ETL script. In the main repo folder:

```bash
poetry run progressphl-data etl --version 2
```



