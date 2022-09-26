# Project Title

## movie search

## Getting Started


This program searches for films according to the specified parameters

## Input arguments

```
-n              number of films

-g              enter the genres you want

-yf             enter the year from

-yt             enter the year to

-rg             name of the ".+name"

-m              Input moves fail
```

## Run example



```
python3 mapper.py -m "data/movies.csv" -g Documentary | sort | python3  reducer.py
```

## Author
Roman Gusarov
