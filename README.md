APOLLO R Package
================

The R package companion of [Assessment of Pre-trained Observational Large Longitudinal models in OHDSI (APOLLO)](https://github.com/OHDSI/Apollo).

Offers the following functionality:
- Extract data from data in the CDM to Parquet files, for use in Apollo.
- Extract features for a cohort to Parquet files, for use in Apollo

## Cloning the package and its submodule

If you want to use the APOLLO Python code you'll need to not only clode this repo but its submodule as well. You can either use

```
git clone --recurse-submodules https://github.com/OHDSI/ApolloR.git
```

to clone in one step, or 

```
git clone --recurse-submodules https://github.com/OHDSI/ApolloR.git
git submodule init
git submodule update
```

to clone in two steps.


## Configuring Python

Assuming the right Python version (3.10) has been installed and is picked up by `reticulate`:

```r
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
