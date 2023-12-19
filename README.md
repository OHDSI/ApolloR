APOLLO R Package
================

The R package companion of [Assessment of Pre-trained Observational Large Longitudinal models in OHDSI (APOLLO)](https://github.com/OHDSI/Apollo).

Offers the following functionality:
- Extract data from data in the CDM to Parquet files, for use in Apollo.
- Extract features for a cohort to Parquet files, for use in Apollo

## Configuring Python

Assuming the right Python version (3.10) has been installed and is picked up by `reticulate`:

```r
# Verify we're using the right Python version:
reticulate::py_config()

# Create virtual environment:
reticulate::virtualenv_create(
  envname = "apollo",
  version = "3.10",
  requirements = ApolloR::getPythonRequirementsFilePath()
)

# Use the virtual environment:
reticulate::use_virtualenv("apollo")
```


Under development. Do not use.
