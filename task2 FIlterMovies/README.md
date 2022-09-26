# Project Title

### Movies filter

# Getting Started


is an application for searching movies by parameters (name, genre, year) in Datasets provided by grouplens
## Input arguments

-h, --help brings up the help menu 

--number number of films for each genre

--genres enter the genres you want

--year_from enter the year from

--year_to enter the year to

--regexp name of the film



## Run example

```
python3 get-movies.py --number 2 --genres "Adventure|Comedy"
```
```
python3 get-movies.py --number 2 --genres "Adventure|Comedy" --year_from 2000
```
```
python3 get-movies.py --number 2 --genres "Adventure|Comedy" --year_from 2000 --year_to 2010
```
```
python3 get-movies.py --number 2 --genres "Adventure|Comedy" --year_from 2000 --year_to 2010 --regexp Tom
```


## Authors

* **Roman Gusarov**