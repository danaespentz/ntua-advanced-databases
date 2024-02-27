import pandas as pd
import geopy.distance

def get_distance(lat1 , long1 , lat2 , long2):
    return geopy.distance.geodesic((lat1 , long1), (lat2 , long2)).km

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

def select_and_save_columns3(input_file, output_file):
    data = pd.read_csv(input_file, skiprows=[0], usecols=[1, 13, 26, 27])
    
    data['Date Rptd'] = pd.to_datetime(data['Date Rptd'])
    data['Date Rptd'] = data['Date Rptd'].dt.strftime("2015-%m-%d")
    
    descent_mapping = {
        'A': 'Other Asian',
        'B': 'Black',
        'C': 'Chinese',
        'D': 'Cambodian',
        'F': 'Filipino',
        'G': 'Guamanian',
        'H': 'Hispanic/Latin/Mexican',
        'I': 'American Indian/Alaskan Native',
        'J': 'Japanese',
        'K': 'Korean',
        'L': 'Laotian',
        'O': 'Other',
        'P': 'Pacific Islander',
        'S': 'Samoan',
        'U': 'Hawaiian',
        'V': 'Vietnamese',
        'W': 'White',
        'X': 'Unknown',
        'Z': 'Asian Indian'
    }
    data = data.dropna(subset=['Vict Descent'])
    data = data[data['Vict Descent'] != 'X'] 
    data['Vict Descent'] = data['Vict Descent'].map(descent_mapping)
    
    revgecoding = pd.read_csv('data/revgecoding.csv', skiprows=[0], names=['LAT', 'LON', 'ZIP'])
    data['LAT'] = data['LAT'].astype(str)
    data['LON'] = data['LON'].astype(str)
    revgecoding['LAT'] = revgecoding['LAT'].astype(str)
    revgecoding['LON'] = revgecoding['LON'].astype(str)
    merged_data = data.merge(revgecoding, on=['LAT', 'LON'], how='left').drop(['LAT', 'LON'], axis=1)

    income = pd.read_csv('data/LA_income_2015.csv', skiprows=[0], names=['ZIP', 'LOC', 'Income'])
    income['Income'] = income['Income'].replace({'\$': '', ',': ''}, regex=True).astype(str)
    income['ZIP'] = income['ZIP'].astype(str)
    income.to_csv('income2015.csv', index=False)
    print(income)

    merged_data['ZIP'] = merged_data['ZIP'].astype(str)
    merged_data = merged_data.merge(income, on=['ZIP'], how='left').drop(['LOC'], axis=1).dropna(subset=['Income'])
    merged_data.to_csv(output_file, index=False)
    print(merged_data)

def select_and_save_columns4(input_file, output_file):
    data = pd.read_csv(input_file, skiprows=[0], usecols=[1, 4, 5, 16, 26, 27]).dropna(subset=['Weapon Used Cd'])
    data['Date Rptd'] = pd.to_datetime(data['Date Rptd'])
    data['Date Rptd'] = data['Date Rptd'].dt.strftime("%Y")
    data['AREA NAME'] = data['AREA NAME'].str.upper()
    print(data)
    police = pd.read_csv('data/LAPD_Police_Stations.csv', skiprows=[1], names=['pLAT', 'FID', 'AREA NAME', 'STATION', 'AREA ', 'pLON'])
    police['pLAT'] = police['pLAT'].round(4)
    police['pLON'] = police['pLON'].round(4)
    print(police)
    data = data.merge(police, on=['AREA ', 'AREA NAME'], how='left').drop(['FID','STATION'], axis=1)
    data = data.drop(data.index[0]).dropna()
    
    data['distance'] = data.apply(lambda row: get_distance(row['LAT'], row['LON'], row['pLAT'], row['pLON']), axis=1)
    distances = []
    for index, row in data.iterrows():
        print(index)
        print(row)
        min_distance = row['distance']
        print(min_distance)
        for p_index, p_row in police.iterrows():
            distance = get_distance(row['LAT'], row['LON'], p_row['pLAT'], p_row['pLON'])
            if distance < min_distance:
                min_distance = distance
        distances.append(min_distance)

    data['min_distance'] = distances
    
    newdata = data[['Date Rptd', 'AREA ', 'AREA NAME', 'Weapon Used Cd', 'distance', 'min_distance']]
    newdata.to_csv(output_file, index=False)
    print(newdata)


# Columns to select for Crime_Data files
crime_columns = ['Date Rptd', 'DATE OCC', 'Vict Age', 'LAT', 'LON']

# Example usage
split_columns('data/Crime_Data_from_2010_to_2019.csv', 'Crime_Data_from_2010_to_2019.csv')
split_columns('data/Crime_Data_from_2020_to_Present.csv', 'Crime_Data_from_2020_to_Present.csv')
split_columns('data/LAPD_Police_Stations.csv', 'LAPD_Police_Stations.csv')

select_and_save_columns('data/Crime_Data_from_2010_to_2019.csv', 'Selected_Crime_Data_from_2010_to_2019.csv', crime_columns)
select_and_save_columns('data/Crime_Data_from_2020_to_Present.csv', 'Selected_Crime_Data_from_2020_to_Present.csv', crime_columns)

select_and_save_columns2('data/Crime_Data_from_2010_to_2019.csv', 'Selected2_Crime_Data_from_2010_to_2019.csv', crime_columns)
select_and_save_columns2('data/Crime_Data_from_2020_to_Present.csv', 'Selected2_Crime_Data_from_2020_to_Present.csv', crime_columns)

select_and_save_columns3('data/Crime_Data_from_2010_to_2019.csv', 'Selected3_Crime_Data_2015.csv')

select_and_save_columns4('data/Crime_Data_from_2010_to_2019.csv', 'Selected4_Crime_Data_from_2010_to_2019.csv')
select_and_save_columns4('data/Crime_Data_from_2020_to_Present.csv', 'Selected4_Crime_Data_from_2020_to_Present.csv')
