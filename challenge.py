# Import dependencies
import json
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine
from config import db_password
import time
import psycopg2

def ETL_pipleline(wiki_data, kaggle_metadata, ratings_data):
    #----------------------------------------------------
    # EXTRACT the data files
    #----------------------------------------------------
    
    # File directory path
    file_dir = 'C:/Users/benni/DataBootcamp/Movies-ETL/'
    
    # extract wikipedia.movies.json
    try:
        # Read the JSON data to get the wiki data.
        with open(f'{file_dir}/{wiki_data}', mode='r') as file:
            wiki_movies_raw = json.load(file)
        print(f"Successfully loaded Wikipedia JSON data.")    
    
    except:
        print(f"Error occured while attempting to load Wikipedia JSON data.")
    
    
    #extract movies_metadata.csv
    try:
        kaggle_metadata = pd.read_csv(f'{file_dir}{kaggle_metadata}', low_memory=False) #low_memory=False removes warning message
        print(f"Successfully loaded Kaggle movies_metadata.csv.")
        
    except:
        print(f"Error occured while attempting to load Kaggle movies_metadata.csv.")
    
    #extract ratings.csv
    try:
        ratings = pd.read_csv(f'{file_dir}{ratings_data}')
        print(f"Successfully loaded ratings.csv.")
        
    except:
        print(f"Error occured while attempting to load ratings.csv.")
        
    #----------------------------------------------------
    # TRANSFORM the data files
    #----------------------------------------------------
    
    # Clean Wikipedia Data
    
    
    # Create a list comprehension with filter expressions so the list contains Wikipedia movies with a Director and imdb_link.
    try:
        wiki_movies = [movie for movie in wiki_movies_raw
                   if ('Director' in movie or 'Directed by' in movie)
                   and 'imdb_link' in movie
                   and 'No. of episodes' not in movie]
        print(f"Filtering wiki_data successful.")

    except:
        print(f"Error occured while attempting to load wiki raw data.")

    # Write a function to clean the wiki data that consolodates data stored in similar columns.
    def clean_movie(movie):
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        # combine alternate titles into one list

        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                'Hangul','Hebrew','Hepburn','Japanese','Literally',
                'Mandarin','McCune-Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian',
                'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles

        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')

        return movie  
    
    # Rerun the list comprehension to clean wiki_movies and recreate wiki_movies_df.
    try:
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        wiki_movies_df = pd.DataFrame(clean_movies)
        print(f"Successfully created DataFrame: wiki_movies_df.")
    except:
        print(f"Error occured while attempting to create DataFrame: wiki_movies_df.")
        

    # Get the IMDb ID using a regular expression (regex)
    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        print(f"Successfully extracted IMDb ID using regex.")
        
    except:
        print(f"Error occured while attempting to extractract IMDb ID using regex.")
    

    # Remove duplicate IMDb information from wiki_movies_df.
    try:
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        print(f"Successfully removed rows with duplicate IMDb IDs.")
        
    except:
        print(f"Error occured while attempting to remove rows with duplicate IMDb IDs.")
        
        
    # Select the columns to keep from the Pandas DataFrame wiki_movies_df (i.e. remove mostly null columns).
    try:
        wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
        wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]
        print(f"Successfully removed columns which have more than 10% null values from wiki_movies_df.")
    
    except:
        print(f"Error occured while attempting to remove columns which have more than 10% null values from wiki_movies_df.")
    
    
    #CLEAN COLUMNS : box_office
    # Make a data series that drops missing values of Box Office data.
    box_office = wiki_movies_df['Box office'].dropna()
    
    # Convert any lists to strings. 
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Define forms
    form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
    form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'    

    # Find values of box_office given as a range and replace with usable format.
    box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # Make a regular expression that captures data when it matches either form_one or form_two
    box_office.str.extract(f'({form_one}|{form_two})')
    
    # Define a function that turns the extracted regex values into numeric values.
    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub('\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub('\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value

        # otherwise, return NaN
        else:
            return np.nan    
    # Extract the values from box_office using str.extract.
    # Apply parse_dollars() to the first column in the DataFrame returned by str.extract
    try:
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        print(f"Successfully converted box_office values to numeric values.")
    
    except:
        print(f"Error occured while attempting to convert box_office values to numeric values.")
        
    # Now that 'box_office' was added as a column to wiki_movies_df, 'Box office' column can be dropped.
    try:
        wiki_movies_df.drop('Box office', axis=1, inplace=True)
        print(f"Successfully removed 'Box Office' from wiki_movies_df.")
    
    except:
        print(f"Error occured while attempting to remove 'Box Office' from wiki_movies_df.")
        
    
    #CLEAN COLUMNS : budget
    # Make a data series that drops missing values of budget data.
    budget = wiki_movies_df['Budget'].dropna()
    
    # Convert any lists to strings
    budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Remove any values between a dollar sign and a hyphen (for budgets given in ranges)
    budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
    
    # Extract the values from budget using str.extract.
    # Apply parse_dollars() to the first column in the DataFrame returned by str.extract
    try:
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        print(f"Successfully converted budget values to numeric values.")
    
    except:
        print(f"Error occured while attempting to convert budget values to numeric values.")
        
    # Drop the 'Budget' column now that we have budget in the correct format
    try:
        wiki_movies_df.drop('Budget', axis=1, inplace=True)
        print(f"Successfully removed 'Budget' from wiki_movies_df.")
    
    except:
        print(f"Error occured while attempting to remove 'Budget' from wiki_movies_df.")
    
    #CLEAN COLUMNS : release date
    # Make a variable that holds the non-null values of Release date in the DataFrame, converting lists to strings
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    # Parse the following forms:
    # 1. Full month name, one- to two-digit day, four-digit year (January 1, 2000)
    # 2. Four-digit year, two-digit month, two-digit day, with any separator (2000-01-01)
    # 3. Full month name, four-digit year (January 2000)
    # 4. Four-digit year (2000)
    date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
    date_form_two = r'\d{4}.[01]\d.[123]\d'
    date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
    date_form_four = r'\d{4}'
    
    try:
        # Extract the dates
        release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)
        
        # Instead of creating a new function to parse the dates, use the built-in to_datetime() method in Pandas.
        # Since there are different date formats, set the infer_datetime_format option to True.
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        print(f"Successfully cleaned release_date data.")
        
    except:
        print(f"Error occured while attempting to clean release_date data.")


    #CLEAN COLUMNS : running time
    # Make a variable that holds the non-null values of Running time in the DataFrame, converting lists to strings
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    
    try:
        # Extract running time.
        # We only want to extract digits, and we want to allow for both possible patterns.
        # Add capture groups around the \d instances and add an alternating character.
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        
        # running_time_extract DataFrame is all strings; Convert them to numeric values.
        # To account for empty strings, use the to_numeric() method and set the errors argument to 'coerce'.
        # Coercing the errors will turn the empty strings into Not a Number (NaN), then we can use fillna() to change all the NaNs to zeros.
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        
        # apply a function that will convert the hour capture groups and minute capture groups to minutes if the pure minutes capture group is zero AND
        # save the output to wiki_movies_df
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        
        # Drop 'Running time'
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
        
        print(f"Successfully cleaned running_time data.")
    
    except:
        print(f"Error occured while attempting to clean running_time data.")

    print(f"Wikipedia DataFrame (wiki_movies_df) has been cleaned.")
    
    
    
    # Clean Kaggle Data
    
        
    try:
        # Drop the adult column from kaggle_data
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
        print(f"Successfully removed 'adult' column from Kaggle data.")
    
    except:
        print(f"Error occured while attempting to remove 'adult' column from Kaggle data.")
        
    
    try:
        # Assign the boolean column back to 'video'.
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        
        # Change column datatypes.
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
        
        print(f"Successfully cleaned Kaggle data.")
    
    except:
        print(f"Error occured while attempting to clean Kaggle data.")

    
    
    # Clean Ratings Data
    
    try:
        # Assign the converted timestamp to the 'timestamp' column in ratings DataFrame.
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        print(f"Successfully cleaned Ratings data.")
    
    except:
        print(f"Error occurred while attempting to clean Ratings data.")
    
    
    
    
    
    # Merge wiki_movies_df and kaggle_metadata
    
    try:
        # Use the suffixes parameter to identify which table each column came from.
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        print(f"Successfully merged wiki_movies_df and kaggle_metadata into movies_df.")
        
    except:
        print(f"Error occurred while attempting to merge wiki_movies_df and kaggle_metadata.")
        
    try:
        # Drop outlier data.
        movies_df = movies_df.drop(movies_df[(movies_df['release_date_wiki'] > '1996-01-01') & (movies_df['release_date_kaggle'] < '1965-01-01')].index)
        print("Successfully removed outliers.")
    
    except:
        print(f"Error occurred while attempting to remove outliers.")
        
    try:
        # Drop the columns with duplicate information.
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
        
        # Make a function that fills in missing data for a column pair and then drops the redundant column.
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column], axis=1)
            df.drop(columns=wiki_column, inplace=True)
        
        # run the function for the three column pairs that we decided to fill in zeros (runtime, bugdet_kaggle, revenue)
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        print(f"Successfully removed duplicate columns and filled missing Kaggle information with Wikipedia information.")
        
    except:
        print(f"Error occured while attempting to remove duplicate columns or fill in missing Kaggle information with Wikipedia information.")
        
    
    try:
        # Reorder the columns in movies_df.
        movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                               'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                               'genres','original_language','overview','spoken_languages','Country',
                               'production_companies','production_countries','Distributor',
                               'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on']]
        
        print(f"Successfully reordered columns in movies_df.")
        
    except:
        print(f"Error occured while attempting ot reorder columns in movies_df.")
        
    
    try:
        # Rename the columns in movies_df.
        movies_df.rename({'id':'kaggle_id',
                          'title_kaggle':'title',
                          'url':'wikipedia_url',
                          'budget_kaggle':'budget',
                          'release_date_kaggle':'release_date',
                          'Country':'country',
                          'Distributor':'distributor',
                          'Producer(s)':'producers',
                          'Director':'director',
                          'Starring':'starring',
                          'Cinematography':'cinematography',
                          'Editor(s)':'editors',
                          'Writer(s)':'writers',
                          'Composer(s)':'composers',
                          'Based on':'based_on'
                         }, axis='columns', inplace=True)
        
        print(f"Successfully renamed columns in movies_df.")
    
    except:
        print(f"Error occurred while attempting to rename columns in movies_df.")
    
    
    
    # Transform Rating Data and merge with movies_df
    try:
        # Pivot this data so that movieId is the index, the columns will be all the rating values, and the rows will be the counts for each rating value.
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                .rename({'userId':'count'}, axis=1) \
                .pivot(index='movieId',columns='rating', values='count')
        
        # Rename the columns so they’re easier to understand.
        # Prepend rating_ to each column with a list comprehension
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
        
        print(f"Successfully transformed Rating Data.")
    
    except:
        print(f"Error occurred while attempting to transform Rating Data.")
    
    try:
        # Merge ratings counts into movies_df.
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        
        # Because not every movie got a rating for each rating level, there will be missing values instead of zeros. Fill those in.
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
        
        print(f"Successfully merged movies_df with rating_counts")
    
    except:
        print(f"Error occurred while attempting to merge movies_df with rating_counts.")
        
     
    
    #----------------------------------------------------
    # LOAD the data into SQL Database
    #----------------------------------------------------
    
    try:
        # Create the database engine
        db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/movie_data"
        engine = create_engine(db_string)
        print(f"Successfully created database engine.")
        
    except:
        print(f"Error creating database engine")
    
    try:
        # Import movie data to Postgres

        # Save the movies_df DataFrame to a SQL table by specifying the name of the table and the engine in the to_sql() method.
        movies_df.to_sql(name='movies', con=engine, if_exists='replace')


        # create a variable for the number of rows imported
        rows_imported = 0

        # get the start_time from time.time()
        start_time = time.time()        

        for data in pd.read_csv(f'{file_dir}{ratings_data}', chunksize=1000000):

            # print out the range of rows that are being imported
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')

            if rows_imported == 0 :
                data.to_sql(name='ratings', con=engine, if_exists='replace')
            else:
                data.to_sql(name='ratings', con=engine, if_exists='append')

            # increment the number of rows imported by the size of 'data'
            rows_imported += len(data)

            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed.')

        # print that the rows have finished importing
        print(f"Successfully imported movies data to Postgresql.")
    
    except:
        print(f"Error occurred while attempting to import movies data to Postgresql.")

# Test the function
# Assign the 3 data files to be taken as arguments in ETL_pipleline().
ETL_pipleline('wikipedia.movies.json', 'movies_metadata.csv', 'ratings.csv')