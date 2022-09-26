# Project Title

### csv-parquet converter 

# Getting Started

This program can convert files from cvs to parquet and vice versa, output the file schema

## Input arguments

-h, --help brings up the help menu 

-i pas to input file

-o pas to output file

-a ACTION { csv2parquet, parquet2csv, get_schema}

### Prerequisites

What things you need to install the software and how to install them

```
pip install -r requirements.txt
```

## Run example

```
python3 converter.py -a <action_name>  -i <path_to_input_file>  -o <path_to_output_file>
```
```
python3 converter.py -a csv2parquet  -i data/test_csv.csv  -o data/test_parquet.parquet
```
```
python3 converter.py -a parquet2csv  -i data/test_parquet.parquet  -o data/test_csv.csv
```
```
python3 converter.py -a get_schema   -i data/test_csv.csv
```
![Alt text](data/screenshot/schemaCsv.png?raw=true "Optional Title")
```
python3 converter.py -a get_schema   -i data/test_parquet.parquet
```
![Alt text](data/screenshot/schemaParquet.png?raw=true "Optional Title")

```
python3 converter.py
```

![Alt text](data/screenshot/argsNo.png?raw=true "Optional Title")


## Authors

* **Roman Gusarov**