## BCG - Big Data Case Study
---

Provide the useful insights using the sample database of vehicle accidents across US for brief amount of time

##### Dataset:

Data Set folder has 6 csv files. Please use the data dictionary (attached in the mail) to understand the dataset and then develop your approach to perform below analytics.

#### Project Structure

```
.
├── src
│   ├── config.yml
│   ├── main.py
│   ├── dependencies.py
│   └── requirements.txt
│   └── analytics
│   │   ├── __init__.py
│   │   └── analysis_body_styles_crashes.py
│   │   ├── analysis_safe_crashes.py
│   │   └── analysis_top_speeding_vehicles.py
│   │   └── analysis_top_state.py
│   │   └── analysis_top_vehicle_crashes.py
│   │   └── analysis_top_zip_codes.py
│   │   └── analysis_total_crashes.py
│   │   └── analysis_two_wheelers.py
│   └── inputs
│       ├── Charges_use.csv
│       └── Damages_use.csv
│       └── Endorse_use.csv
│       └── Primary_Person_use.csv
│       └── Restrict_use.csv
│       └── Units_use.csv
│   └── utils
│       └── __init__.py
│       └── schemas.py
│       └── utils.py
│       └── logger.py
│   └── tests
|       └── __init__.py
|       └── test_files
|          └── test.csv
|       └── test_main.py
|       └── test_utils.py
├── README.md

```



##### Analytics:
##### Application should perform below analysis and store the results for each analysis.

Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?

Analysis 2: How many two wheelers are booked for crashes?

Analysis 3: Which state has highest number of accidents in which females are involved?

Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)



#### Expected Output:

* Develop an application which is modular & follows software engineering best practices (e.g. Classes, docstrings, functions, config driven, command line executable through spark-submit)

* Code should be properly organized in folders as a project.

* Input data sources and output should be config driven

* Code should be strictly developed using Dataframe APIs (Do not use Spark SQL)

#### Steps to run the analysis:

##### Prerequisite

* PySpark
* Python 3.7.x

##### Dependencies: 
* PyYAML==5.4.1

#### Input file

In this use case, I have used the yaml, to configure the input files directory.

Update the input file directory in the yaml file and file names.

* One place to change the input directory and if any files names updates

Note: No need to handle in code, if any files name gets renamed


### Execution Steps [Local]:

Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?

        spark-submit --master local[*] main.py --pipeline total_crashes --output_file_path <path> --output_format <fileformat>
        
Analysis 2: How many two wheelers are booked for crashes?
    
        spark-submit --master local[*] main.py --pipeline total_two_wheelers_crashes --output_file_path <path> --output_format <fileformat>

Analysis 3: Which state has highest number of accidents in which females are involved?
        
        spark-submit --master local[*] main.py --pipeline top_states_crashes --output_file_path <path> --output_format <fileformat>        

Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        spark-submit --master local[*] main.py --pipeline top_vehicles_crashes --output_file_path <path> --output_format <fileformat>

Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

        spark-submit --master local[*] main.py --pipeline body_style_crashes --output_file_path <path> --output_format <fileformat>

Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

        spark-submit --master local[*] main.py --pipeline top_zip_codes_crashes --output_file_path <path> --output_format <fileformat>        

Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    
        spark-submit --master local[*] main.py --pipeline safe_crashes --output_file_path <path> --output_format <fileformat>

Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
    
        spark-submit --master local[*] main.py --pipeline top_speeding_vehicles_crashes --output_file_path <path> --output_format <fileformat>

