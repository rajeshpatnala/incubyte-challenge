# Incubyte Challenge - Vaccination Statistics

## Dependencies
| Tools/Packages      					| Version                                                 				|
|-------------------------------------------------------|---------------------------------------------------------------------------------------|
| python       					| 3.8.10 				|
| pyspark       					| 3.1.2 				|
| com.crealytics.spark.excel       					| 2.12:0.13.4 				|

## About the Dataset
This dataset has vaccination details for different Nationals India, Australia and USA.

### Input Files
The data in csv, xlsx formats are listed in input_files folder.


### Output
These are the attributes that we will use
| Attribute      					| Description                                                 				|
|-------------------------------------------------------|---------------------------------------------------------------------------------------|
| country       					| Name of the Nation from file name. 				|
| VaccinationType   				|  Type of Vaccination							|



### Cleaned Data
Cleaned data will be inside [output_files] as a CSV file inside with the following columns.

1. [output_files/vaccination_count](output_files/vaccination_count) [subdirectory] 
    - country
    - vaccination_type
    - count

2. [output_files/vaccination_percentage](output_files/vaccination_percentage) [subdirectory] 
    - country
    - vaccinated_percentage

3. [output_files/vaccination_contribution](output_files/vaccination_contribution) [subdirectory] 
    - country
    - vaccination_contribution

### Setting up Python Environment

The below command helps to create python environment.

`virtualenv spark_env`

Activate spark_env

`source spark_env/bin/activate`

Install Required Modules

`pip3 install -r requirements.txt`


### Import Procedure

The below script will clean the data in **input_files**, Also generate final files.

`python3 process.py`

Using Spark Submit

`spark-submit --packages com.crealytics:spark-excel_2.12:0.13.4 --master local[2] --py-files constants.py process.py`

### Running Tests

Run the test cases

`python3 -m unittest process_test.py`


