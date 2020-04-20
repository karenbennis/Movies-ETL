# Movies-ETL
This repository contains the ETL code for an online movie streaming service.

## Project Overview
The goal of this project was to create a Python script that automates the process of extracting, transforming, and loading Wikipedia movie data (JSON format), Kaggle metadata, and MovieLens rating data.

This repository includes two jupyter notebooks wherein ETL code was written.
* Movielens_Extract.ipynb
* Challenge.ipynb

In Movielens_Extract.ipynb, the iterative process of cleaning data takes place with exploratory analysis documented throughout. The data is inspected for problems; after identifying problems a plan for a fix was devised, and finally, the repair was executed.

Challenge.ipynb takes all the ETL decisions and defines a function that automates the ETL. This code is documented in challenge.py as well.


## Challenge
The goals of the challenge are to:

* Create an automated ETL pipeline.
* Extract data from multiple sources.
* Clean and transform the data automatically using Pandas and regular expressions.
* Load new data into PostgreSQL.

Directions for the ETL pipeline are as follows:

1. Create a function that takes in three arguments (Wikipedia data, Kaggle metadata, MovieLens rating data (from Kaggle))
2. Use the code from Movielens_Extract.ipynb so that the function performs all of the transformation steps. Remove any exploratory data analysis and redundant code.
3. Add the load steps from Movielens_Extract.ipynb to the function. Remove the existing data from SQL, but keep the empty tables.
4. Check that the function works correctly on the current Wikipedia and Kaggle data.
5. Document any assumptions that are being made. Use try-except blocks to account for unforeseen problems that may arise with new data.

## Resources
* Data Sources: movies_metadata.csv, ratings.csv, wikipedia.movies.json
* Software/Tools: pgAdmin 4 (v 4.19), Quick DBD (quickdatabasediagrams.com)
* Databases: Jupyter Lab 1.2.6, PostgreSQL 11.7
* Languages: Python, SQL

## Summary
This section lists the assumptions and overall recommendations surrounding the ETL_pipeline() function.

### Assumptions
The ETL_pipeline() function works in accordance with the following assumptions:
* Wikipedia raw data is in .json format
* Kaggle raw data (movies_metadata and ratings) are in .csv format
* The raw data is current and has not been modified since Movielens_Extract.ipynb was created
* Decisions regarding data transformation are consistent with those made in Movielens_Extract.ipynb
* The user provides inputs for the ETL function in the correct order (i.e.'wikipedia.movies.json', 'movies_metadata.csv', 'ratings.csv')
* The user's file directory matches the path in the function
* The data is being loaded into existing table names (i.e.'movies', 'ratings'), using the existing connection string

### Observations / Recommendations
This analysis demonstrates the power of ETL to handle large amounts of data. The function ETL_pipeline() renders the same tables as the code in Movielens_Extract.ipynb. Since the ETL_pipeline() function involves many blocks of code, various blocks were created using try-except blocks which print the respective block's success or failure message. This allows for quick troubleshooting for errors / typos. In this respect, the ETL_pipeline() function is more overtly informative than the exploratory code found in Movielens_Extract.ipynb.

While the goal of this analysis is to automate the ETL, if any of the raw data files were to be updated, the robust cleaning processes would need to be revisted, as it would no longer be safe to assume the comprehensiveness of the analysis.

As a recommendation, it is suggested that each time the raw data is modified, the analysis would need to be revisted. The implications of this are that the ETL_pipeline() function can only be automated so long as the raw data is current. To ensure that the analysis is indeed fresh, it is recommended that the code be revisted each time the raw data has been modified.