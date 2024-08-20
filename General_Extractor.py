
import csv
import warnings
from csv import writer
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, Table, MetaData
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus

load_dotenv() # specify the path to the .env file
APP_FILES_PATH = "App_Files"



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
# engine = create_engine(f'mssql+pyodbc:///?odbc_connect={connection_string}')
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

def extract_general(year,file,dictionary_sheets,table_names):
    for key, value in table_names.items():
        locals()[key] = value


    APP_FILES_PATH = "App_Files" #Obtains the file path for the files

    # Function Checks if keywords are in a dataframe
    def contains_all_keywords(data,keywords):
        return all(keyword.lower() in " ".join(map(str,data.stack().values.tolist())).lower() for keyword in keywords)


    # extract the data
    data = {}
    ## Read the name of the excel file in the folder and the worksheets contained
    
    file = pd.ExcelFile(file)
    worksheets = file.sheet_names
    


    ## Obtain the names of the work sheets required

    ### general insurance required appendices
    bs = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general', 'balance sheet'])]

    rv = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general','combined revenue'])]

    npc = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general','summary of net paid claims'])]

    ic = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general','summary of net incurred claims'])]

    nep = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general','summary of net earned premiums'])]   

    bif = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['general','summary of business in force'])]


    ### life insurance required appendices


    # bs_life = [sheet for sheet in worksheets if 'Content' not in sheet
    #     if contains_all_keywords(dictionary_sheets[sheet],['long term', 'balance sheet'])]

    # rv_life = [sheet for sheet in worksheets if 'Content' not in sheet
    #     if contains_all_keywords(dictionary_sheets[sheet],['long term business','revenue'])]

    # bif_life = [sheet for sheet in worksheets if 'Content' not in sheet
    #     if contains_all_keywords(dictionary_sheets[sheet],['long term','summary of business in force'])]

    # app_6 = [sheet for sheet in worksheets if 'Content' not in sheet
    #     if contains_all_keywords(dictionary_sheets[sheet],['long term business','direct premium'])]

    # app_7 = [sheet for sheet in worksheets if 'Content' not in sheet
    #     if contains_all_keywords(dictionary_sheets[sheet],['long term business','inward reinsurance premium'])]   

    # app_8 = [sheet for sheet in worksheets if 'Content' not in sheet
    #     if contains_all_keywords(dictionary_sheets[sheet],['long term business','outward reinsurance premium'])]

    # app_18 = [sheet for sheet in worksheets if 'Content' not in sheet
    #         if contains_all_keywords(dictionary_sheets[sheet],['long term','actuarial valuations'])]





    ### COLUMN RENAMER FUNCTION
    def col_renamer(cols):
        cols = cols.str.replace("_x000D_", "")
        cols = cols.str.replace("\n", "")
        cols = cols.str.strip()
        cols = cols.str.replace("  ", " ")
        cols = cols.str.upper()
        cols = cols.str.replace(" ", "_")
        cols = cols.str.replace("_/_", "/")
        cols = cols.str.replace("_/", "/")
        cols = cols.str.replace("_X000D_", "")
        cols = cols.str.replace('_.1', '.1')
        return cols

    ### COMPANY RENAMER
    def company_renamer(x):
        x = x.str.replace("INSURANCE KENYA", "")
        x = x.str.replace("ASSURANCE COMPANY", "")
        x = x.str.replace("INSURANCE COMPANY", "")
        x = x.str.replace("INSURANE COMPANY", "")
        x = x.str.replace("ASSURANCE", "")
        x = x.str.replace("INSURANCE", "")
        x = x.str.replace("LIMITED", "")
        x = x.str.replace("GENERAL", "")
        x = x.str.replace("COPORATE", "CORPORATE")
        x = x.str.strip()
        x = x.str.replace("  ", "_")
        x = x.str.replace(" ", "_")
        x = x.str.replace("  ", "")

        return x

    ### BALANCE SHEET

    ### only works for 2019 to 2017 since they were already in excel format
    years = year
    files = [file]
    gen_df = pd.DataFrame()

    for i in bs:
        ##reading specific sheets
        variable = f'sheet{bs.index(i)+1}'
        d = pd.DataFrame(dictionary_sheets[i])
        header = d.index[d.eq('Company').any(axis=1)][0]
        locals()[variable] = pd.read_excel(file,i,header=header+1)
        ### dropping the unnamed column and set Company as index
        locals()[variable].drop("Unnamed: 0", axis=1, inplace=True)
        locals()[variable].set_index("Company", inplace=True)
        ##add a column for year and rename columns and concatenate
        locals()[variable] = locals()[variable].T
        locals()[variable]['years'] = years
        locals()[variable].columns = col_renamer(locals()[variable].columns)
        gen_df = pd.concat([gen_df,locals()[variable] ])
    gen_df.reset_index(inplace=True)
    gen_df.rename(columns={"index": "Company"}, inplace=True)

    gen_df.columns = col_renamer(gen_df.columns)

    # for fil in files:
    #     years = years
    #     f1 = pd.ExcelFile(fil)
    #     ## reading the specific sheets
    #     sheet1 = pd.read_excel(f1, bs[0], header=2)
    #     sheet2 = pd.read_excel(f1, bs[1], header=4)
    #     sheet3 = pd.read_excel(f1, bs[2], header=4)
    #     sheet4 = pd.read_excel(f1, bs[3], header=4)
    #     ### dropping the unnamed column
    #     sheet1.drop("Unnamed: 0", axis=1, inplace=True)
    #     sheet2.drop("Unnamed: 0", axis=1, inplace=True)
    #     sheet3.drop("Unnamed: 0", axis=1, inplace=True)
    #     sheet4.drop("Unnamed: 0", axis=1, inplace=True)
    #     ### setting the df index
    #     sheet1.set_index("Company", inplace=True)
    #     sheet2.set_index("Company", inplace=True)
    #     sheet3.set_index("Company", inplace=True)
    #     sheet4.set_index("Company", inplace=True)
    #     ### adding year col
    #     sheet1 = sheet1.T
    #     sheet2 = sheet2.T
    #     sheet3 = sheet3.T
    #     sheet4 = sheet4.T
    #     sheet1["years"] = years
    #     sheet2["years"] = years
    #     sheet3["years"] = years
    #     sheet4["years"] = years
    #     #### fixing the unqoted shares
    #     sheet1.columns = col_renamer(sheet1.columns)
    #     sheet2.columns = col_renamer(sheet2.columns)
    #     sheet3.columns = col_renamer(sheet3.columns)
    #     sheet4.columns = col_renamer(sheet4.columns)
    #     # ### concatinating the data sheets
    #     gen_df = pd.concat([gen_df, sheet1, sheet2, sheet3, sheet4])
    # gen_df.reset_index(inplace=True)
    # gen_df.rename(columns={"index": "Company"}, inplace=True)

    # gen_df.columns = col_renamer(gen_df.columns)

    gen_df = gen_df[['COMPANY', 'SHARE_CAPITAL',
                        'SHARE_PREMIUM', 'REVALUATION_RESERVES',
                        'STATUTORY_RESERVES', 'RETAINED_EARNINGS',
                        'OTHER_RESERVES', 'TOTAL_EQUITY',
                        'UNDERWRITING_PROVISIONS', 'ACTUARIAL_CONTRACT_LIABILITIES',
                        'LONGTERM_LIABILITIES', 'CURRENT_LIABILITIES',
                        'TOTAL_EQUITY_AND_LIABILITIES', 'LAND_AND_BUILDINGS',
                        'INVESTMENT_PROPERTY', 'OTHER_FIXED_ASSETS',
                        'GOVERNMENT_SECURITIES', 'OTHER_SECURITIES',
                        'INVESTMENT_IN_RELATED_COMPANIES', 'CORPORATE_BONDS',
                        'COMMERCIAL_PAPERS', 'DEBENTURES',
                        'ORDINARY_SHARES_QUOTED', 'ORDINARY_SHARES_UNQUOTED',
                        'PREFERENCE_SHARES_QUOTED', 'PREFERENCE_SHARES_UNQUOTED',
                        'LOANS_SECURED_&_UNSECURED', 'MORTGAGES',
                        'TERM_DEPOSITS', 'CASH_AND_CASH_BALANCES',
                        'OUTSTANDING_PREMIUMS', 'OTHER_RECEIVABLES',
                        'OTHER_ASSETS', 'INTANGIBLE_ASSETS',
                        'TOTAL_ASSETS', "YEARS"]]

    gen_df

    #### NET EARNED PREMIUMS

    years = year

    files = [files[0]]
    NEP_gen_19_17 = pd.DataFrame()

    for i in nep:
        ##reading specific sheets
        variable = f'sheet{nep.index(i)+1}'
        d = pd.DataFrame(dictionary_sheets[i])
        header = d.index[d.eq('Company').any(axis=1)][0]
        locals()[variable] = pd.read_excel(file,i,header=header+1)
        ### dropping the unnamed column and set Company as index
        locals()[variable].drop("Unnamed: 0", axis=1, inplace=True)
        ##add a column for year and rename columns and concatenate
        locals()[variable]['years'] = years
        NEP_gen_19_17 = pd.concat([NEP_gen_19_17, locals()[variable]])
    
  

    NEP_gen_19_17.set_index("Company", inplace=True)

    NEP_gen_19_17 = NEP_gen_19_17.loc['INSURERS':'REINSURERS', ][
        ['Aviation ', 'Engineering', 'Fire Domestic', 'Fire Industrial',
            'Liability', 'Marine', 'Motor Private', 'Motor Commercial',
            'Motor Commercial PSV', 'Personal Accident', 'Theft',
            "Workmens' Compensation", 'Medical', 'Miscellaneous', 'Total_x000D_\n', "years"]]
    NEP_gen_19_17.columns = ["NET_EARNED_PREMIUM_" + i for i in NEP_gen_19_17.columns]
    NEP_gen_19_17.columns = col_renamer(NEP_gen_19_17.columns)

    #### REVENUE STATEMENT

    years = year
    files = [files[0]]
    rev_20_17 = pd.DataFrame()
    for i in rv:
        ##reading specific sheets
        variable = f'sheet{rv.index(i)+1}'
        d = pd.DataFrame(dictionary_sheets[i])
        header = d.index[d.eq('Company').any(axis=1)][0]
        locals()[variable] = pd.read_excel(file,i,header=header+1)
        ### dropping the unnamed column and set Company as index
        locals()[variable].drop("Unnamed: 0", axis=1, inplace=True)
        ##add a column for year and rename columns and concatenate
        locals()[variable]['years'] = years
        rev_20_17 = pd.concat([rev_20_17, locals()[variable]])


    rev_20_17.set_index("Company", inplace=True)
    # rev_20_17.columns = ["REV_"+i for i in rev_20_17.columns]
    rev_20_17 = rev_20_17.loc["INSURERS":"REINSURERS", ]

    ### NET PAID CLAIMS
    years = year
    files = [files[0]]
    claims_gen_19_17 = pd.DataFrame()
    for i in npc:
        ##reading specific sheets
        variable = f'sheet{npc.index(i)+1}'
        d = pd.DataFrame(dictionary_sheets[i])
        header = d.index[d.eq('Company').any(axis=1)][0]
        locals()[variable] = pd.read_excel(file,i,header=header+1)
        ### dropping the unnamed column and set Company as index
        locals()[variable].drop("Unnamed: 0", axis=1, inplace=True)
        ##add a column for year and rename columns and concatenate
        locals()[variable]['years'] = years
        claims_gen_19_17 = pd.concat([claims_gen_19_17, locals()[variable]])

  
    claims_gen_19_17.set_index("Company", inplace=True)

    claims_gen_19_17.loc["INSURERS":"REINSURERS", ]

    Claims_gen_19_17 = claims_gen_19_17.loc["INSURERS":"REINSURERS", ]

    claims_gen_19_17.columns = ["NET_PAID_CLAIMS_" + i for i in claims_gen_19_17.columns]
    claims_gen_19_17.columns = col_renamer(claims_gen_19_17.columns)

    ### BUSINESS IN FORCE
    years = year
    files = [files[0]]
    BIN_gen_19_17 = pd.DataFrame()
    
    for i in bif:
        ##reading specific sheets
        variable = f'sheet{bif.index(i)+1}'
        d = pd.DataFrame(dictionary_sheets[i])
        header = d.index[d.eq('Company').any(axis=1)][0]
        locals()[variable] = pd.read_excel(file,i,header=header+1)
        ### dropping the unnamed column and set Company as index
        locals()[variable].drop("Unnamed: 0", axis=1, inplace=True)
        ##add a column for year and rename columns and concatenate
        locals()[variable]['years'] = years
        BIN_gen_19_17 = pd.concat([BIN_gen_19_17, locals()[variable]])

    
    
    
    
    
 
    #### AUTOMAYING COLUMN SELECTION BASED ON DIFFERENT YEARS
    col_name_ls = []
    for col in BIN_gen_19_17.columns.str.lower():
        if ((col != "company") & (col != "years")):
            col_name_ls.append(col)

    col_name_ls

    col_n = len(col_name_ls) / 2

    col_names_2 = ["COMPANY"] + ["BUSINESS_IN_FORCE_New_" + i for i in col_name_ls[0:int(col_n)]] + [
        "BUSINESS_IN_FORCE_Total_" + i for i in col_name_ls[int(col_n):]] + ["YEARS"]

    # col_names = ["COMPANY"]+["BUSINESS_IN_FORCE_New_"+i for i in BIN_gen_19_17.columns[1:4]]+["BUSINESS_IN_FORCE_Total_"+i for i in BIN_gen_19_17.columns[4:7]]+["YEARS"]

    BIN_gen_19_17.columns = col_names_2

    BIN_gen_19_17.set_index("COMPANY", inplace=True)
    BIN_gen_19_17.columns = BIN_gen_19_17.columns.str.replace(".1", "")
    BIN_gen_19_17.columns = BIN_gen_19_17.columns.str.replace("*", "")
    BIN_gen_19_17 = BIN_gen_19_17.loc[:"TOTAL", ]

    BIN_gen_19_17.columns = col_renamer(BIN_gen_19_17.columns)

    BIN_gen_19_17

    ####INCURRED CLAIMS
    years = year
    files = [files[0]]
    IC_gen_19_17 = pd.DataFrame()
    
    for i in ic:
        ##reading specific sheets
        variable = f'sheet{ic.index(i)+1}'
        d = pd.DataFrame(dictionary_sheets[i])
        header = d.index[d.eq('Company').any(axis=1)][0]
        locals()[variable] = pd.read_excel(file,i,header=header+1)
        ### dropping the unnamed column and set Company as index
        locals()[variable].drop("Unnamed: 0", axis=1, inplace=True)
        ##add a column for year and rename columns and concatenate
        locals()[variable]['years'] = years
        IC_gen_19_17 = pd.concat([IC_gen_19_17, locals()[variable]])

    
    IC_gen_19_17.set_index("Company", inplace=True)

    IC_gen_19_17.columns = ["INCURRED_CLAIMS_" + i for i in IC_gen_19_17.columns]

    IC_gen_19_17 = IC_gen_19_17.loc["INSURERS":"REINSURERS", ]

    IC_gen_19_17.columns = col_renamer(IC_gen_19_17.columns)

    IC_gen_19_17

    ##### resetting the index to get common column
    IC_gen_19_17.reset_index(inplace=True)

    NEP_gen_19_17.reset_index(inplace=True)

    BIN_gen_19_17.reset_index(inplace=True)

    claims_gen_19_17.reset_index(inplace=True)

    rev_20_17.reset_index(inplace=True)

    ##### setting all columns to uppercase
    IC_gen_19_17.columns = IC_gen_19_17.columns.str.upper()
    gen_df.columns = gen_df.columns.str.upper()
    BIN_gen_19_17.columns = BIN_gen_19_17.columns.str.upper()
    rev_20_17.columns = rev_20_17.columns.str.upper()
    NEP_gen_19_17.columns = NEP_gen_19_17.columns.str.upper()
    claims_gen_19_17.columns = claims_gen_19_17.columns.str.upper()

    ### HATRMONIZING COMPANY NAMES
    IC_gen_19_17.COMPANY = company_renamer(IC_gen_19_17.COMPANY)
    gen_df.COMPANY = company_renamer(gen_df["COMPANY"])
    BIN_gen_19_17.COMPANY = company_renamer(BIN_gen_19_17.COMPANY)
    rev_20_17.COMPANY = company_renamer(rev_20_17["COMPANY"])
    NEP_gen_19_17.COMPANY = company_renamer(NEP_gen_19_17.COMPANY)
    claims_gen_19_17.COMPANY = company_renamer(claims_gen_19_17.COMPANY)

    ##### RENAMING ALL THE YEAR COLUMNS
    IC_gen_19_17.rename(columns={"INCURRED_CLAIMS_YEARS": "YEARS"}, inplace=True)
    claims_gen_19_17.rename(columns={"NET_PAID_CLAIMS_YEARS": "YEARS"}, inplace=True)
    rev_20_17.rename(columns={"REV_YEARS": "YEARS"}, inplace=True)
    NEP_gen_19_17.rename(columns={"NET_EARNED_PREMIUM_YEARS": "YEARS"}, inplace=True)
    BIN_gen_19_17.rename(columns={"BUSINESS_IN_FORCE_YEARS": "YEARS"}, inplace=True)

    ##### DATA MERGING
    df_merged = pd.merge(BIN_gen_19_17, rev_20_17, on=["COMPANY", "YEARS"])

    df_merged = pd.merge(df_merged, claims_gen_19_17, on=["COMPANY", "YEARS"])

    df_merged = pd.merge(df_merged, gen_df, on=["COMPANY", "YEARS"])

    df_merged = pd.merge(df_merged, NEP_gen_19_17, on=["COMPANY", "YEARS"])

    df_merged = pd.merge(df_merged, IC_gen_19_17, on=["COMPANY", "YEARS"])

    df_merged.columns = col_renamer(df_merged.columns)

    gen_df
    ##### CONVERSION OF DATA TO NUMERIC
    for col in df_merged.columns:
        if (col != "COMPANY"):
            try:
                df_merged[col] = df_merged[col].str.replace(",", "")
                df_merged[col] = pd.to_numeric(df_merged[col])
            except AttributeError:
                continue

    ##### CONVERSION OF DATA TO NUMERIC
    for col in df_merged.columns:
        if (col != "COMPANY"):
            try:
                # df_merged[col]= df_merged[col].str.replace(",","")
                df_merged[col] = pd.to_numeric(df_merged[col])
            except TypeError:
                continue


    ##### SELECTING INSURANCE COMPANY DATA

    df_merged.set_index("COMPANY", inplace=True)

    final = df_merged.drop(["TOTAL"], axis=0)

    final.columns = col_renamer(final.columns)

    final.rename(
        {'AAR': "AAR INSURANCE KENYA LTD", 'AFRICAN_MERCHANT': "AFRICAN MERCHANT ASSURANCE COMPANY LTD",
            'AIG': "AIG INSURANCE COMPANY LTD",
            'APA': "APA INSURANCE LTD", 'BRITAM': "BRITAM GENERAL INSURANCE",
            'CANNON': 'CANNON ASSURANCE LTD', 'CIC': 'CIC GENERAL INSURANCE LTD',
            'CORPORATE': 'CORPORATE INSURANCE COMPANY', 'DIRECTLINE': 'DIRECTLINE ASSURANCE',
            'FIDELITY_SHIELD': 'FIDELITY SHIELD INSURANCE COMPANY LTD',
            'FIRST': "FIRST ASSURANCE COMPANY LTD", 'GA': "GA LIFE ASSURANCE LIMITED",
            'GATEWAY': 'GATEWAY INSURANCE', 'GEMINIA': 'GEMINIA INSURANCE COMPANY LTD',
            'HERITAGE': "HERITAGE INSURANCE COMPANY KENYA LTD",
            'ICEA_LION': 'ICEA LION GENERAL INSURANCE COMPANY LTD',
            'INTRA-AFRICA': 'INTRA-AFRICA INSURANCE COMPANY LTD',
            'INVESCO': 'INVESCO ASSURANCE COMPANY LTD', 'JUBILEE': "JUBILEE INSURANCE COMPANY",
            'KENINDIA': 'KENINDIA ASSURANCE COMPANY LTD', 'KENYA_ORIENT': 'KENYA ORIENT INSURANCE COMPANY',
            'MADISON': 'MADISON INSURANCE COMPANY LTD', 'MAYFAIR': 'MAYFAIR INSURANCE COMPANY LTD',
            'MERCANTILE': 'MERCANTILE INSURANCE COMPANY', 'OCCIDENTAL': 'OCCIDENTAL INSURANCE COMPANY LTD',
            'PACIS': 'PACIS INSURANCE COMPANY LTD',
            'PHOENIX_OF_EAST_AFRICA': 'PHOENIX OF EAST AFRICA ASSURANCE COMPANY',
            'REAL': 'REAL INSURANCE COMPANY', 'RESOLUTION': 'RESOLUTION INSURANCE COMPANY',
            'TAKAFUL_OF_AFRICA': 'TAKAFUL INSURANCE OF AFRICA LTD', 'TAUSI': 'TAUSI ASSURANCE COMPANY LTD',
            'THE_KENYAN_ALLIANCE': "THE KENYAN ALLIANCE INSURANCE COMPANY LTD",
            'THE_MONARCH': "THE MONARCH INSURANCE COMPANY LTD",
            'TRIDENT': 'TRIDENT INSURANCE COMPANY LTD', 'UAP': 'UAP INSURANCE COMPANY',
            'XPLICO': 'XPLICO INSURANCE COMPANY LTD', 'SAHAM': 'SAHAM INSURANCE COMPANY LTD',
            'ALLIANZ': 'ALLIANZ INSURANCE COMPANY OF KENYA', 'JUBILEE_HEALTH': 'JUBILEE HEALTH INSURANCE',
            'METROPOLITAN_CANNON': "METROPOLITAN CANNON INSURANCE", 'MUA': 'MUA INSURANCE COMPANY',
            'PIONEER': "PIONEER INSURANCE COMPANY LTD", 'SANLAM': 'SANLAM INSURANCE COMPANY LTD ',
            'TAKAFUL__OF_AFRICA': 'TAKAFUL INSURANCE OF AFRICA LTD'}, inplace=True)

    # final.to_csv(str(year)+"_general.csv")

    final.reset_index(inplace=True)
   
    # download the data for year extracted
    # final.to_csv(str(year)+".csv")


    ### original data for the database

    # GENERAL
    if not table_exists(locals()['general_data']):
        whole_gen_data = pd.read_csv(os.path.join(APP_FILES_PATH,'original_files_dont_delete' ,'general_data.csv'))
    else:
        whole_gen_data = pd.read_sql('SELECT * FROM ' + locals()['general_data'], engine)
        
    ### data addition after mining

    # GENERAL
    app_data = whole_gen_data

    growing_data = pd.concat([app_data, final])

    growing_data = growing_data.loc[:, ~growing_data.columns.str.contains('Unnamed')]



    # try:
    #     # Begin a transaction
    #     with db_session.begin():
    #         # Your DataFrame to SQL operation
    #         growing_data.to_sql(locals()['general_data'], con=db_session.bind, if_exists='replace', index=False)

    #         # Other operations can go here
    # except Exception as e:
    #     db_session.rollback()  # Roll back on exception
    #     raise
    # finally:
    #     db_session.close()  # Ensure the session is closed when done

    growing_data.to_sql(locals()['general_data'], con=engine, if_exists='replace', index=False)


    data = growing_data

    return data