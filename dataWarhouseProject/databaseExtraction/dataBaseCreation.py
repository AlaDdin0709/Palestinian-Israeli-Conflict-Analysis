import pandas as pd 
from datetime import datetime

def is_row_exist(rows_info, row, columns_to_compare_on_it):
    #rows is a list of dict
    for row_info in rows_info:
        if (row[columns_to_compare_on_it] == row_info['row'][columns_to_compare_on_it]).all():
            return row_info['id']


#this function is used to create a primary key for data frame with a column name we provide 
def create_primary_key(df, columnName, columns_to_compare_on_it):
    duplicated_rows = []#contains the bool value of the duplicated rows
    non_duplicated_rows = []

    #find duplicated rows
    duplicated_rows = df.duplicated(subset=columns_to_compare_on_it)
    df['is_duplicated'] = duplicated_rows

    #addition of the primary key to the df
    primary_key = 1
    for index, row in df.iterrows():
        if row['is_duplicated'] == False:

            non_duplicated_row = dict()

            non_duplicated_row["row"] = row
            non_duplicated_row["id"] = index
            
            non_duplicated_rows.append(non_duplicated_row)
            primary_key += 1
    
    df = df.drop('is_duplicated', axis=1)

    primary_keys = []
    for index, row in df.iterrows():
        primary_keys.append(is_row_exist(non_duplicated_rows, row, columns_to_compare_on_it))

    df[columnName] = primary_keys

    return df


def main():
    data_set = pd.read_csv('../fatalities_isr_pse_conflict_2000_to_2023.csv')
    data_set = data_set.head(400)

    #addition of an id to the data_set to create the seperated tables 
    data_set_id = []#will contains the id of each row
    for id in range(1, len(data_set) + 1) :
        data_set_id.append(id)

    data_set['id'] = data_set_id

    # knowing more the db
    #checking the meaning of dates here
    # for dateDiff in zip(data_set.date_of_death, data_set.date_of_event):

    #     print( datetime.strptime(dateDiff[0], "%Y-%m-%d") - datetime.strptime(dateDiff[1], "%Y-%m-%d"))

    #removing the took_part_in_the_hostilities column because it do not contains all the informations and most of its cells are empty

    filtred_data_set = pd.DataFrame()#it contains the new data set that we gonna work with it

    for columnName in data_set.columns :
        if columnName != 'took_part_in_the_hostilities' or columnName != 'notes' :
            filtred_data_set[columnName] = data_set[columnName]


    #seperation of the data into tables 
    person = pd.DataFrame()#this table will contain the information about the person
    person_origin = pd.DataFrame()#this table will contain the origin of the person 
    residence = pd.DataFrame()#this table will contain the residence of the person with more details
    event_location = pd.DataFrame()#this table will contain the location of the event that happen with more details
    event_date = pd.DataFrame()#this table will contain the date frame of the event that happend and the day of death of the person
    killing_info = pd.DataFrame()#this table will contain the information about the type of death of that person and the weapon used

    for columnName in filtred_data_set.columns:
        if(columnName == 'name' or columnName == 'age' or columnName == 'gender'):
            person[columnName] = filtred_data_set[columnName]

            #addition of the id if we haven't added it
            if 'id' not in person.columns :
                person['id'] = filtred_data_set['id']
        elif(columnName == 'event_location_region' or columnName == 'event_location_district' or columnName == 'event_location'):
            
            if columnName == 'event_location_region':
                event_location["region"] = filtred_data_set[columnName]
            elif columnName == 'event_location_district':
                event_location["district"] = filtred_data_set[columnName]
            else :
                event_location['place'] = filtred_data_set[columnName]

            #addition of the id if we haven't added it
            if 'id' not in event_location.columns :
                event_location['id'] = filtred_data_set['id']

        elif(columnName == 'place_of_residence' or columnName == 'place_of_residence_district'):
            
            if columnName == 'place_of_residence_district':
                residence["district"] = filtred_data_set[columnName]
            else :
                residence['place'] = filtred_data_set[columnName]

            #addition of the id if we haven't added it
            if 'id' not in residence.columns :
                residence['id'] = filtred_data_set['id']

        elif(columnName == 'ammunition' or columnName == 'killed_by' or columnName == 'type_of_injury'):
            killing_info[columnName] = filtred_data_set[columnName]

            #addition of the id if we haven't added it
            if 'id' not in killing_info.columns :
                killing_info['id'] = filtred_data_set['id']
        
        elif (columnName == 'date_of_event' or columnName == 'date_of_death'):
            event_date[columnName] = filtred_data_set[columnName]

            #addition of the id if we haven't added it
            if 'id' not in event_date.columns :
                event_date['id'] = filtred_data_set['id']
        
        elif columnName == 'citizenship' : #addition of the origin of the person
            person_origin['country'] = filtred_data_set[columnName]

            #addition of the id if we haven't added it
            if 'id' not in person_origin.columns :
                person_origin['id'] = filtred_data_set['id']

    #creation of primary keys for the tables

    person_origin = create_primary_key(person_origin, 'origin_id', ['country'])
    event_location = create_primary_key(event_location, 'event_location_id', ['region', 'district', 'place'])
    event_date = create_primary_key(event_date, 'event_date_id', ['date_of_death', 'date_of_event'])
    residence = create_primary_key(residence, 'residence_id', ['place', 'district'])
    killing_info = create_primary_key(killing_info, 'killing_info_id', ['type_of_injury', 'killed_by', 'ammunition'])

    #saving tables 
    person_origin.to_csv('person_origin.csv', index=False)
    event_location.to_csv('event_location.csv', index=False)
    event_date.to_csv('event_date.csv', index=False)
    residence.to_csv('residence.csv', index=False)
    killing_info.to_csv('killing_info.csv', index=False)
    person.to_csv('person.csv', index=False)

main()

