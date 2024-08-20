import csv
from urllib.parse import quote_plus
import warnings
import numpy as np
from csv import writer
import pandas as pd
import os
import re

from sqlalchemy import create_engine, inspect,MetaData
from General_Extractor import extract_general
from Life_Extractor import extract_life
from General_Ratio import calculate_general_ratio
from Life_Ratio import calculate_life_ratio

from dotenv import load_dotenv
load_dotenv() # specify the path to the .env file
APP_FILES_PATH = "App_Files"
from sqlalchemy.orm import sessionmaker



#Obtain database information and for an engine using .env

ODBC_Driver = os.environ.get("ODBC_Driver")
Server=os.environ.get("Server")
Database=os.environ.get("Database")
Uid=os.environ.get("Uid")
Pwd=os.environ.get("Pwd")



connection_string = f'DRIVER={ODBC_Driver};SERVER={Server};DATABASE={Database};UID={Uid};PWD={Pwd}'
encoded_connection_string = quote_plus(connection_string)

# Create the SQLAlchemy engine
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={encoded_connection_string}')

# engine = create_engine(f'mssql+pymssql://{Uid}:{Pwd}@{Server}/{Database}')
db_Session = sessionmaker(bind=engine)
db_session = db_Session()


def table_list():
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return list(metadata.tables.keys())

def table_exists(table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine)
    return table_name in metadata.tables.keys()







def process_data(year, file,dictionary_sheets,table_names):

    # -------------------------------------------------------

    def create_company_dictionary_names():
        # Function Checks if keywords are in a dataframe
        def contains_all_keywords(data,keywords):
            return all(keyword.lower() in " ".join(map(str,data.stack().values.tolist())).lower() for keyword in keywords)


        # extract the data
        data = {}
        ## Read the name of the excel file in the folder and the worksheets contained
        
        Ex_file = pd.ExcelFile(file)
        worksheets = Ex_file.sheet_names

        ## Obtain the names of the work sheets required





        # Filter sheet names based on keywords and create lists of names
        names_1 = [sheet for sheet in worksheets if 'Content' not in sheet
                if sheet in dictionary_sheets and contains_all_keywords(dictionary_sheets[sheet],['directory', 'companies'])]
        names_2 = [sheet for sheet in worksheets if 'Content' not in sheet
                if sheet in dictionary_sheets and contains_all_keywords(dictionary_sheets[sheet],['directory', 'group'])]

        # Combine unique sheet names from both lists into a single list
        selected_names = list(set(names_1) | set(names_2))

        # Select sheet names containing the specified pattern (APPENDIX followed by optional Roman numerals)
        selected_sheets = [sheet for sheet in selected_names if 'Content' not in sheet
                        if re.search(r'APPENDIX\s+\d+\s*(?:[IVXLCDM]+)?', sheet)]


        code_dict = {}
        if selected_sheets !=[]:
            
            # Create an empty DataFrame
            df = pd.DataFrame()

            # Iterate through each sheet in selected_sheets
            for sheet in selected_sheets:
                # Read the Excel sheet into a DataFrame
                sheet1 = pd.read_excel(Ex_file, sheet, header=0)
                
                # Find the index of the column containing the string 'directory' (case-insensitive)
                col = sheet1.columns.get_loc(sheet1.astype(str).apply(lambda x: x.str.contains('directory', case=False)).any().idxmax())
                
                # Extract columns located after the 'directory' column, only keep the next two columns
                sheet1 = sheet1.iloc[:, col + 1:col + 3]
                
                # Remove rows with all NaN values
                sheet1.dropna(how='all', inplace=True)
                
                # Rename columns as 'COMPANY' and 'TYPE'
                sheet1.columns = ['COMPANY', 'TYPE']
                
                # Concatenate the current sheet DataFrame with the overall DataFrame 'df'
                df = pd.concat([df, sheet1])

            # Convert values in the 'COMPANY' and 'TYPE' columns to uppercase
            df['COMPANY'] = df['COMPANY'].str.upper()
            df['TYPE'] = df['TYPE'].str.upper()

            # Remove duplicate rows in the DataFrame
            df.drop_duplicates(inplace=True)

            # Filter out rows where the 'TYPE' column contains the substring 'reinsur' (case-insensitive)
            df = df[-df['TYPE'].str.contains('reinsur', case=False)]

            # Filter out rows where the 'COMPANY' column is equal to "COMPANY" or "GROUP"
            df = df[(df['COMPANY'] != "COMPANY") & (df['COMPANY'] != 'GROUP')]

            # Remove spaces from values in the 'TYPE' column
            df['TYPE'] = df['TYPE'].str.replace(" ", "")

            def create_short_name_insurance(string):
                # Split the string into words and assign the first two words to results and results2
                results = string.split()[0]
                results2 = string.split()[1]
                
                # Check if the first word is 'THE', 'KENYA', or 'KENYAN'
                if results == 'THE' or results == 'KENYA' or results == "KENYAN":
                    # If so, assign the second and third words to results and results2
                    results = string.split()[1]
                    results2 = string.split()[2]
                    
                    # Check again if the first word is 'THE', 'KENYA', or 'KENYAN'
                    if results == 'THE' or results == 'KENYA' or results == "KENYAN":
                        # If so, assign the third and fourth words to results and results2
                        results = string.split()[2]
                        results2 = string.split()[3]
                        
                # Check if the second word is in the specified set of keywords
                if results2 in {'INSURANCE', 'LIFE', 'KENYA', 'KENYAN', 'LIFE', 'GENERAL', 'ASSURANCE', 
                            'HEALTH', 'COMPANY', 'HOLDINGS', 'GROUP', 'MICRO', 'LIMITED', '(K)','HOLDING',
                            'INVESTMENT','INVESTMENTS'}:
                    # If so, set results2 to an empty string
                    results2 = ""
                
                # Return the modified results and results2
                return results, results2

            # Apply the create_short_name_insurance function to create new columns 'CODE' and 'CODE2'
            df['CODE'], df['CODE2'] = zip(*df['COMPANY'].apply(create_short_name_insurance))

            # Concatenate 'CODE' and 'CODE2' columns with a space in between
            df['CODE2'] = df['CODE'] + " " + df['CODE2']

            # Strip leading and trailing whitespaces from the 'CODE2' column
            df['CODE2'] = df['CODE2'].str.strip()

            # Sort the DataFrame by the 'CODE' column
            df = df.sort_values(by='CODE')

            # Identify duplicates in the 'CODE' column
            duplicate_codes = df.duplicated(subset=['CODE'], keep=False)

            # Filter out rows with duplicates in the 'CODE' column and specified conditions in the 'TYPE' column
            df = df[~((df['TYPE'].isin(['GENERAL', 'LONGTERM', 'LIFE'])) & duplicate_codes)]

            # Create a dictionary from the 'CODE' and 'COMPANY' columns
            code_dict = dict(zip(df['CODE'], df['COMPANY']))
        return code_dict

    # ----------------------------------------------------
    
    # Unpack the dictionary of table_names into variables and values
    for key, value in table_names.items():
        locals()[key] = value



    # Extract data from life_file and general_file
    life_data_ = extract_life(year, file,dictionary_sheets,table_names)
    general_data_ = extract_general(year, file,dictionary_sheets,table_names)

    # Calculate ratios
    life_ratio_ = calculate_life_ratio(year, life_data_,table_names)
    general_ratio_ = calculate_general_ratio(year, general_data_,table_names)
    ####### LIFE AND GENERAL MERGING

    df_life = life_ratio_
    df_gen = general_ratio_

    dfg = df_gen[["COMPANY", "YEARS", 'INSURANCE_SCORE', 'RATING']]
    dfl = df_life[["COMPANY", "YEARS", 'INSURANCE_SCORE', 'RATING']]

    dfg = dfg[-(dfg["COMPANY"] == "TOTAL")]
    dfl = dfl[-(dfl["COMPANY"] == "TOTAL")]

    dfg[dfg["COMPANY"].str.contains("coporate", case=False)]
    dfl[dfl["COMPANY"].str.contains("coporate", case=False)]

    dfc = pd.concat([dfg, dfl])
    def create_name(string):
        results = \
            string.split('LIFE')[0].split('ASSURANCE')[0].split('INSURANCE')[0].split('GENERAL')[0].split(
                'HEALTH')[0]
        return results

    # create a code for the

    def create_short_name_insurance(string):
        results = string.split()[0]
        if results == 'THE' or results == 'KENYA' or results == "KENYAN":
            results = string.split()[1]
            if results == 'THE' or results == 'KENYA' or results == "KENYAN":
                results = string.split()[2]
        return results

    dct = {'AAR': 'AAR INSURANCE COMPANY LTD',
           'ABSA': 'ABSA LIFE ASSURANCE KENYA LIMITED',
           'AIG': 'AIG KENYA INSURANCE COMPANY LTD',
           'AFRICA': 'AFRICA MERCHANT ASSURANCE COMPANY LTD',
           'ALLIANCE': 'THE KENYAN ALLIANCE INSURANCE COMPANY LIMITED',
           'APA': 'APOLLO INVESTMENTS LIMITED',
           'BRITAM': 'BRITAM HOLIDING PLC',
           'BARCLAYS': 'BARCLAYS LIFE ASSURANCE',
           'CIC': 'CIC INSURANCE GROUP LIMITED',
           'CAPEX': 'CAPEX LIFE ASSSURANCE COMPANY LIMITED',
           'CORPORATE': 'CORPORATE INSURANCE COMPANY LIMITED',
           'COPORATE': 'CORPORATE INSURANCE COMPANY LIMITED',
           'DIRECTLINE': 'DIRECTLINE ASSURANCE COMPANY LTD',
           'EQUITY': 'EQUITY LIFE ASSURANCE (KENYA) LIMITED',
           'FIDELITY': 'FIDELITY SHIELD INSURANCE COMPANY LTD',
           'FIRST': 'FIRST ASSURANCE COMPANY LIMITED',
           'GA': 'GA INSURANCE LIMITED GROUP',
           'GEMINIA': 'GEMINIA INSURANCE CO. LTD',
           'HERITAGE': 'THE HERITAGE INSURANCE COMPANY LIMITED',
           'ICEA': 'ICEA LION INSURANCE HOLDINGS LTD',
           'INTRA': 'INTRA AFRICA ASSURANCE COMPANY LIMITED',
           'INVESCO': 'INVESCO ASSURANCE COMPANY LTD',
           'JUBILEE': 'JUBILEE HOLDINGS LIMITED',
           'KENINDIA': 'KENINDIA ASSURANCE COMPANY LIMITED',
           'KUSCCO': 'KUSCCO MUTUAL ASSURANCE LIMITED',
           'LIBERTY': 'LIBERTY KENYA HOLDINGS PLC',
           'MUA': 'THE MUA GROUP',
           'MADISON': 'MADISON GROUP LIMITED',
           'MAYFAIR': 'MAYFAIR INSURANCE COMPANY LIMITED',
           'METROPOLITAN': 'METROPOLITAN CANNON GENERAL INSURANCE LIMITED',
           'MONARCH': 'THE MONARCH INSURANCE COMPANY LIMITED',
           'OCCIDENTAL': 'OCCIDENTAL INSURANCE COMPANY LTD',
           'OLD': 'OLD MUTUAL HOLDINGS PLC',
           'ORIENT': 'KENYA ORIENT INSURANCE  LIMITED',
           'PACIS': 'PACIS INSURANCE COMPANY LTD',
           'PIONEER': 'PIONEER GENERAL INSURANCE COMPANY',
           'PRUDENTIAL': 'PRUDENTIAL LIFE ASSURANCE COMPANY LIMITED',
           'RESOLUTION': 'RESOLUTION GROUP LIMITED',
           'SANLAM': 'SANLAM KENYA PLC',
           'TAKAFUL': 'TAKAFUL INSURANCE OF AFRICA LTD',
           'TAUSI': 'TAUSI ASSURANCE COMPANY LIMITED',
           'TRIDENT': 'TRIDENT INSURANCE COMPANY LIMITED',
           'XPLICO': 'XPLICO INSURANCE COMPANY LTD',
           'CANNON': 'CANNON ASSURANCE LTD',
           'SAHAM': 'SAHAM INSURANCE COMPANY LTD',
           'PHOENIX': 'PHOENIX OF EAST AFRICA ASSURANCE COMPANY'}

    dfc['GRADE'] = np.where(dfc["RATING"] == 'A', 1,
                            np.where(dfc["RATING"] == 'B', 2, np.where(dfc["RATING"] == 'C', \
                                                                       3,
                                                                       np.where(dfc["RATING"] == 'D', 4,
                                                                                np.nan))))

    dfc["YEARS"] = dfc["YEARS"].astype("int")

    dfc['CODE'] = dfc['COMPANY'].apply(lambda x: create_short_name_insurance(x))

    ####    *********************************************************
    dfc.loc[dfc.CODE.str.contains('UAP', case=False) & (dfc.YEARS >= 2018), ['CODE']] = 'OLD'
    dfc.loc[dfc.CODE.str.contains('APA', case=False) & (dfc.YEARS >= 2018), ['CODE']] = 'APOLLO'
    #####  ***************************************************************
    dfc.loc[dfc.CODE.str.contains('KUSCO', case=False), ['CODE']] = 'KUSCCO'
    # dfc.loc[dfc.COMPANY.str.contains('JUBILEE LIFE', case = False),['CODE']]= 'JUBILEE LIFE'
    # dfc.loc[dfc.COMPANY.str.contains('JUBILEE HEALTH', case = False),['CODE']]= 'JUBILEE HEALTH'
    # dfc.loc[dfc.COMPANY.str.contains('JUBILEE GENERAL', case = False),['CODE']]= 'JUBILEE ALLIANZ'
    dfc.loc[dfc.COMPANY.str.contains('INTRA', case=False), ['CODE']] = 'INTRA'

    ### *******************************************************************
    dfc.loc[
        (dfc.COMPANY.str.contains('ALLIANZ', case=False)) & (dfc.YEARS >= 2020), ['CODE']] = 'JUBILEE'

    ### *********************************************************************
    code_dict = create_company_dictionary_names()
    dfc['YEARS'] = dfc['YEARS'].astype('string')
    # dfc.loc[dfc.COMPANY.str.contains('ALLIANZ', case = False),['CODE']]= 'JUBILEE ALLIANZ'
    dfc.loc[dfc.COMPANY.str.contains('AFRICAN', case=False), ['CODE']] = 'AFRICA'
    dfc.loc[dfc.CODE.str.contains('coporate', case=False), ['CODE']] = 'CORPORATE'
    dfc.loc[dfc.CODE.str.contains('BRITISH', case=False), ['CODE']] = 'BRITAM'
    dfc['CODE1'] = dfc['CODE'] + '-' + dfc['YEARS'].str[-2:]
    dfc['NAME'] = dfc['COMPANY'].apply(lambda x: create_name(x)) + ' INSURANCE COMPANY'
    dfc['NAME'] = dfc['COMPANY'].apply(lambda x: create_name(x)) + ' INSURANCE COMPANY'
    dfc = dfc.sort_values(['CODE1', 'GRADE'], ascending=False)
    dfc = dfc.drop_duplicates(subset='CODE1', keep='first')
  
    dfc["NAME"] = dfc['CODE'].map(lambda x: dct.get(x, dfc.loc[dfc['CODE'] == x, 'NAME'].iloc[0]))
    dfc["NAME"] = dfc['CODE'].map(lambda x: code_dict.get(x, dfc.loc[dfc['CODE'] == x, 'NAME'].iloc[0]))

    dfc = dfc.drop(['CODE1'], axis=1)
    dfc = dfc.sort_values(["YEARS", "RATING", "COMPANY"], ascending=False)
    

    ####### saving the resuting files in a results folder as a file

    # result_path = "/var/www/scoringapp/services/users/NCBA_FILES/App_Files/Resulting_Files"
    #
    # CurrentYear = max(dfc['YEARS'].astype(int))
    #
    # dfc1 = dfc[dfc['YEARS'].astype(int) == CurrentYear]
    #
    # file_name_combined = str(year) + "_combined_general_life_rating.csv"
    #
    # file_path_combined = os.path.join(result_path, file_name_combined)
    #
    # dfc1 = dfc1.loc[:, ~dfc1.columns.str.contains('Unnamed')]
    #
    # dfc1.to_csv(file_path_combined)


    # to slice for the latest year use:
    # CurrentYear = max(dfc['YEARS'].astype(int))
    # dfc1 = dfc[dfc['YEARS'].astype(int) == CurrentYear]

    # combined_file_path1 = '/var/www/scoringapp/services/users/NCBA_FILES/App_Files/App_Files/combined_general_life_rating.csv'
    # combined_file_path1 = '/var/www/scoringapp/services/users/NCBA_FILES/App_Files/combined_general_life_rating.csv' # -pk

    # combined_file_path1 = 'combined_general_life_rating.csv' # +pk
    file_path = os.path.join(APP_FILES_PATH, 'combined_general_life_rating.csv') # +pk

    dfc = dfc.loc[:, ~dfc.columns.str.contains('Unnamed')]

    if not table_exists(locals()['combined_rating']):
        dfc[['COMMENT', 'RECOMMENDATION', 'ADJUSTMENT','ADJUSTED_GRADE']] = ''
        dfc['ADJUSTED_RATING'] = dfc['RATING']
        dfc = dfc.sort_values(['NAME', 'YEARS'])
        dfc['ADJUSTED_RATING_Yr_1'] = dfc.groupby(['NAME'])['ADJUSTED_RATING'].shift(1)
        dfc = dfc.sort_values(["YEARS",'ADJUSTED_RATING','ADJUSTED_RATING_Yr_1','NAME'],
                                ascending = [False,True,True,True]).reset_index().drop('index',axis=1).reset_index()
        dfc['SORT'] = dfc['index'].rank()
        dfc = dfc.drop(['index'], axis =1)
        combined_file = dfc

        # try:
        #     # Begin a transaction
        #     with db_session.begin():
        #         # Your DataFrame to SQL operation
        #         combined_file.to_sql(locals()['combined_rating'], con=db_session.bind, if_exists='replace',index=False)

        #         # Other operations can go here
        # except Exception as e:
        #     db_session.rollback()  # Roll back on exception
        #     raise
        # finally:
        #     db_session.close()  # Ensure the session is closed when done

        combined_file.to_sql(locals()['combined_rating'], con=engine, if_exists='replace',index=False)



    else:
        combined_file = pd.read_sql('SELECT * FROM ' + locals()['combined_rating'], engine)
        # combined_file.to_csv(f'C:/Users/Paul/Desktop/trial/test_combFile_{year}.csv')
        dfc[['COMMENT', 'RECOMMENDATION', 'ADJUSTMENT','ADJUSTED_GRADE']] = ''
        dfc['ADJUSTED_RATING'] = dfc['RATING']
        # dfc.to_csv(f'C:/Users/Paul/Desktop/trial/test_dfc_adjust_{year}.csv')
        dfc = dfc[dfc['YEARS'].astype(int) == int(year)]
        # dfc.to_csv(f'C:/Users/Paul/Desktop/trial/test_filter_{year}.csv')
        dfc = pd.concat([combined_file,dfc])
        # dfc.to_csv(f'C:/Users/Paul/Desktop/trial/test_concat_{year}.csv')

        dfc = dfc.sort_values(['NAME', 'YEARS'])
        dfc['ADJUSTED_RATING_Yr_1'] = dfc.groupby(['NAME'])['ADJUSTED_RATING'].shift(1)
        dfc = dfc.sort_values(["YEARS",'ADJUSTED_RATING','ADJUSTED_RATING_Yr_1','NAME'],
                                ascending = [False,True,True,True]).reset_index().drop('index',axis=1).reset_index()
        dfc['SORT'] = dfc['index'].rank()
        dfc = dfc.drop(['index'], axis =1)
        combined_file = dfc
        # combined_file.to_csv(f'C:/Users/Paul/Desktop/trial/test_final_{year}.csv')

        # try:
        #     # Begin a transaction
        #     with db_session.begin():
        #         # Your DataFrame to SQL operation
        #         combined_file.to_sql(locals()['combined_rating'], con=db_session.bind, if_exists='replace',index=False)

        #         # Other operations can go here
        # except Exception as e:
        #     db_session.rollback()  # Roll back on exception
        #     raise
        # finally:
        #     db_session.close()  # Ensure the session is closed when done


        combined_file.to_sql(locals()['combined_rating'], engine, if_exists='replace',index=False)

    # Save the merged_combined_df to the merged_combined.csv file
    # combined_file.to_sql(combined_rating, engine,index=False, if_exists='replace')



    result= combined_file

    return result