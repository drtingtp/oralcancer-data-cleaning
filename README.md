# Oral Cancer Screening Data Verification
Oral Health Program, Ministry of Health, Malaysia

This app parses a list of MS Access database files (`input` folder), performs verification using a set of [validation rules](../docs/rules.md), finally outputs list of datapoints which failed validation as Excel files (`output` folder).

## Technical details
* [Architecture of app](docs/architecture.md)
* [Validation rules](docs/rules.md)

## Requirements
* Windows x64 OS
* For ODBC driver for MS Access, either:
    * MS Office x64 (with MS Access); OR
    * [Microsoft Access Database Engine 2016 Redistributable x64](https://www.microsoft.com/en-gb/download/details.aspx?id=54920) (Note: cannot be installed on computer with MS Office x86)

## Installation
* Install python / miniforge
* Set up virtual environment using `environment.yml`
* Initialise repo with gitconfig script in `/script`

## Limitation
This app is unable to perform the following validations:
* Name-related validation. E.g. Name vs Gender, Name vs Ethinicity
* Categorisation of string input. E.g. standardisation of medical history terms