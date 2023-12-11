import pandas as pd

def split_columns(input_file, output_file):
    # Read the CSV file into a DataFrame
    data = pd.read_csv(input_file, dtype=str)

    # Split the single column into multiple columns based on commas
    new_columns = data[0].str.split(',', expand=True)

    # Concatenate the new columns with the original DataFrame
    concatenated = pd.concat([data, new_columns], axis=1)

    # Save the new DataFrame with split columns to a new CSV file
    concatenated.to_csv(output_file, index=False)

    # Print the table
    print(concatenated.head())

def select_and_save_columns(input_file, output_file, selected_columns):
    data = pd.read_csv(input_file, skiprows=[0], usecols=[1, 2, 11, 26, 27])
    data['Date Rptd'] = pd.to_datetime(data['Date Rptd'])
    data['Date Rptd'] = data['Date Rptd'].dt.strftime("%Y-%m-%d")
    data['DATE OCC'] = pd.to_datetime(data['DATE OCC'])
    data['DATE OCC'] = data['DATE OCC'].dt.strftime("%Y-%m-%d")
    data.to_csv(output_file, index=False)
    print(data)

def select_and_save_columns2(input_file, output_file, selected_columns):
    data = pd.read_csv(input_file, skiprows=[0], usecols=[3, 15])
    i=0
    for t in data['TIME OCC']:
        if int(t)>=500 and int(t)<1200:
            t="morning"
        elif int(t)>=1200 and int(t)<1700:
            t="noon"
        elif int(t)>=1700 and int(t)<2100:
            t="afternoon"
        elif int(t)>=2100 or int(t)<500:
            t="night" 
        data['TIME OCC'][i]=t
        i=i+1
    data.to_csv(output_file, index=False)
    print(data)

# Columns to select for Crime_Data files
crime_columns = ['Date Rptd', 'DATE OCC', 'Vict Age', 'LAT', 'LON']

# Select and save columns for 'Crime_Data_from_2010_to_2019.csv'
#select_and_save_columns('data/Crime_Data_from_2010_to_2019.csv', 'Selected_Crime_Data_from_2010_to_2019.csv', crime_columns)

# Select and save columns for 'Crime_Data_from_2020_to_Present.csv'
#select_and_save_columns('data/Crime_Data_from_2020_to_Present.csv', 'Selected_Crime_Data_from_2020_to_Present.csv', crime_columns)

select_and_save_columns2('data/Crime_Data_from_2010_to_2019.csv', 'Selected2_Crime_Data_from_2010_to_2019.csv', crime_columns)
#select_and_save_columns2('data/Crime_Data_from_2020_to_Present.csv', 'Selected2_Crime_Data_from_2020_to_Present.csv', crime_columns)

# Example usage
#split_columns('data/Crime_Data_from_2010_to_2019.csv', 'Crime_Data_from_2010_to_2019.csv')
#split_columns('data/Crime_Data_from_2020_to_Present.csv', 'Crime_Data_from_2020_to_Present.csv')
#split_columns('data/LAPD_Police_Stations.csv', 'LAPD_Police_Stations.csv')