import pandas as pd
from sqlalchemy import create_engine
import yaml

# README:
#This python pipeline generates two distinct tables: 
# one for customers with solar panels (solar_indicators) and another for those without (non_solar_indicators)
# Please make sure to add your MySQL credentials to a config.yml file before running this script
# The data sources come from: 
# contracts_eae.csv: Individual contract details 
# meteo_eae.csv: Meteorological data by zipcode
# zipcode_eae_v2.csv: mapping data by zipcode

# Load credentials from the config yaml file
with open("config.yml", "r") as f:
    config = yaml.safe_load(f)

db_config = config['database-1']

connection_str = (
    f"mysql+pymysql://{db_config['user']}:{db_config['password']}@"
    f"{db_config['host']}:{db_config['port']}/{db_config['schema']}"
)

#Create the main pipeline
def run_pipeline():
    #OBS: the csv files have already been cleaned and gone through a quick sanity check
    #loading the csv files
    print('Loading data...')
    contracts = pd.read_csv('contracts_eae.csv')
    meteo = pd.read_csv('meteo_eae.csv', sep=';')
    zipcodes = pd.read_csv('zipcode_eae_v2.csv')

    #turning all column names to lowercase to standardize
    contracts.columns = [col.lower().strip() for col in contracts.columns]
    meteo.columns = [col.lower().strip() for col in meteo.columns]
    zipcodes.columns = [col.lower().strip() for col in zipcodes.columns]

    #Filtering
    #Filtering client type so we can minimize the dataframe size 
    contracts = contracts[contracts['client_type_id'] == 0]

    #Count customers per zip code in the contracts table
    zip_counts = contracts['zipcode'].value_counts()
    significant_zips = zip_counts[zip_counts > 10].index

    #keeping only relevant zipcodes in the tables
    contracts = contracts[contracts['zipcode'].isin(significant_zips)]
    meteo = meteo[meteo['zipcode'].isin(significant_zips)]

    #Power classifier:
    def classify_power(p1):
        if p1 > 5000:
            return 'Power over 5000 kW'
        elif 3000 <= p1 <= 5000:
            return 'Power between 3000 kW and 5000 kW'
        else:
            return 'Power under 3000 kW'
    
    contracts['power_category'] = contracts['power_p1'].apply(classify_power)
    
    #Merging the dataframes: Join meteo and contracts tables
    #Joining on zipcode to associate weather data with contract locations.
    combined_df = pd.merge(contracts, meteo, on='zipcode')
    
    #Create the DB engine
    engine = create_engine(connection_str)

    #Converting the date column datatype to date format
    combined_df['date'] = pd.to_datetime(combined_df['date'])

    #Extract year and month from the date to satisfy the grouping requirements
    combined_df['year'] = combined_df['date'].dt.year
    combined_df['month'] = combined_df['date'].dt.month

    #Aggregation
    #We create a function to handle the aggregation for both solar and non-solar
    def process_and_save(df, table_name):
        #Grouping by year, month, zipcode, and power category
        #The aggregation calculates max/min temp, avg humidity and contract count
        aggregated = df.groupby(['zipcode', 'year', 'month', 'power_category']).agg({
            'temperature': ['max', 'min'],
            'relative_humidity': 'mean',
            'contract_id': 'count'
        }).reset_index()

        #Flatten the multi-index columns to match the required structure
        aggregated.columns = [
            'zipcode', 'year', 'month', 'power_category', 
            'max_temperature', 'min_temperature', 'avg_rel_humidity', 'n'
        ]

        #Output: Inserting into MySQL database
        print(f"Saving data to table: {table_name}...")
        aggregated.to_sql(table_name, con=engine, if_exists='replace', index=False)

    #Separate Solar and Non-Solar customers. Obs: 1 == True and 0 == False
    solar_df = combined_df[combined_df['has_solar'] == 1]
    non_solar_df = combined_df[combined_df['has_solar'] == 0]

    process_and_save(solar_df, 'solar_indicators')
    process_and_save(non_solar_df, 'non_solar_indicators')

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()

