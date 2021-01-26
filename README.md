## Mini SQL Engine-


### About the project-
It is a restricted SQL engine that processes SQL commands provided in the command line interface and applies those on a relational database to produce desired output.

### Features implemented-
* Query clauses supported- `SELECT <column(s)> FROM <table(s)> WHERE <conditions> GROUP BY <column> ORDER BY <column>;`
* Tables are stored in csv files as `<tablename>.csv`
* The schema of all tables present in the database is stored in a file named `metadata.txt`
* Aggregate functions are supported
* Extensive error handling of invalid queries
* Queries are case insensitive (except table names and columns)
### Limitations-
* GROUP BY and ORDER BY can only be applied on one column as of now.
* WHERE clause can support only one occurance of LOGICAL OR/AND. Ex- `Select * FROM Table1 WHERE condition1 AND/OR condition2;` as of now.
* HAVING clause has not been implemented as of now.
#### Dependency to run the code
* Machine should be able to run .py source file
* modules `sqlparse` and `itertools` should be installed (pip install sqlparse and pip install itertools)

#### How to use the engine-
* Make .csv files with the filenames same as the tablenames.
* Make a file named `metadata.txt` containing the schema of the whole database in the following format-<br>
<begin_table> <br>
<table_name1> <br>
<attribute_11> <br>
.... <br>
<attribute_1N> <br>
<end_table> <br>
<begin_table> <br>
<table_name2> <br>
<attribute_21> <br>
.... <br>
<attribute_2N> <br>
<end_table> <br>
and so on for each table.. <br>
* Open a terminal with root of the application folder as the present working directory
* run the bash script with respective queries as its command line argument: `./run.sh "<query>;"`
* NOTE: Sample csv files and metadata.txt with required format is provided.
* NOTE: all queries should end with a semicolon.


#### Illustrations of the working-
Executing some queries:

![demo1_sql](https://user-images.githubusercontent.com/30933610/105897604-6396d400-603e-11eb-81fa-e94d2dea9039.jpg)


Examples of error handling:
![demo2_sql](https://user-images.githubusercontent.com/30933610/105897055-a99f6800-603d-11eb-99c2-69a815c2945d.jpg)

#### Language used-
Python

