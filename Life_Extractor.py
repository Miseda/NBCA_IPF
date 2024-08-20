import csv
from urllib.parse import quote_plus
import warnings
from csv import writer
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.orm import sessionmaker

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




def extract_life(year,year_present,dictionary_sheets,table_names):
    for key, value in table_names.items():
        locals()[key] = value
 
    APP_FILES_PATH = "App_Files" #Obtains the file path for the files

    # Function Checks if keywords are in a dataframe
    def contains_all_keywords(data,keywords):
        return all(keyword.lower() in " ".join(map(str,data.stack().values.tolist())).lower() for keyword in keywords)


    # extract the data
    data = {}
    ## Read the name of the excel file in the folder and the worksheets contained
    files = year_present
    file = pd.ExcelFile(files)
    worksheets = file.sheet_names
    


    ## Obtain the names of the work sheets required

    
    ### life insurance required appendices


    bs_life = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['long term', 'balance sheet'])]

    rv_life = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['long term business','revenue'])]

    bif_life = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['long term','summary of business in force'])]

    app_6 = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['long term business','direct premium'])]

    app_7 = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['long term business','inward reinsurance premium'])]   

    app_8 = [sheet for sheet in worksheets if 'Content' not in sheet
        if contains_all_keywords(dictionary_sheets[sheet],['long term business','outward reinsurance premium'])]

    app_18 = [sheet for sheet in worksheets if 'Content' not in sheet
            if contains_all_keywords(dictionary_sheets[sheet],['long term','actuarial valuations'])]



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

    ### COLUMN RENAMER FUNCTION
    def col_renamer_life(cols):
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
    def company_renamer_life(x):
        x = x.str.replace("INSURANCE KENYA", "")
        x = x.str.replace("ASSURANCE COMPANY", "")
        x = x.str.replace("INSURANCE COMPANY", "")
        x = x.str.replace("INSURANE COMPANY", "")
        x = x.str.replace("ASSURANCE", "")
        x = x.str.replace("INSURANCE", "")
        x = x.str.replace("LIMITED", "")
        x = x.str.replace("GENERAL", "")
        x = x.str.replace("LIFE ASSURANCE COMPANY", "")
        x = x.str.replace("LIFE ASSURANCE", "")
        # x=x.str.replace("LIFE KENYA","")
        x = x.str.strip()
        x = x.str.replace("  ", " ")

        x = x.str.replace(" ", "_")
        x = x.str.replace("__", "_")
        x = x.str.replace("_-", "-")
        return x

    ### BALANCE SHEET

    ### only works for 2019 to 2017 since they were already in excel format

    years = year
    files = [file]
    gen_df = pd.DataFrame()

    for fil in files:
        years = years
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, bs_life[0], header=3)
        sheet2 = pd.read_excel(f1, bs_life[1], header=3)
        sheet3 = pd.read_excel(f1, bs_life[2], header=4)

        ### dropping the unnamed column
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet2.drop("Unnamed: 0", axis=1, inplace=True)
        sheet3.drop("Unnamed: 0", axis=1, inplace=True)

        ### setting the df index
        sheet1.set_index("Company", inplace=True)
        sheet2.set_index("Company", inplace=True)
        sheet3.set_index("Company", inplace=True)

        ### adding year col
        sheet1 = sheet1.T
        sheet2 = sheet2.T
        sheet3 = sheet3.T

        sheet1["years"] = years
        sheet2["years"] = years
        sheet3["years"] = years

        #### fixing the unqoted shares
        sheet1.columns = col_renamer_life(sheet1.columns)
        sheet2.columns = col_renamer_life(sheet2.columns)
        sheet3.columns = col_renamer_life(sheet3.columns)

        # ### concatinating the data sheets
        gen_df = pd.concat([gen_df, sheet1, sheet2, sheet3])

    gen_df.reset_index(inplace=True)
    gen_df.rename(columns={"index": "Company"}, inplace=True)

    gen_df.columns = col_renamer(gen_df.columns)

    gen_df = gen_df[-gen_df["COMPANY"].str.contains("reinsurance", case=False)]

    gen_df.drop(['AMOUNTS_IN_THOUSAND_SHILLINGS'], axis=1, inplace=True)

    gen_df["COMPANY"] = company_renamer_life(gen_df["COMPANY"])

    gen_df.set_index("COMPANY", inplace=True)

    gen_df = gen_df.loc[:"TOTAL", ]

    gen_df.reset_index(inplace=True)
    gen_df.rename(columns={'index': 'COMPANY'})

    #### REVENUE STATEMENT

    years = year
    files = [files[0]]
    rev_20_17 = pd.DataFrame()
    c = 0
    for fil in files:
        years = years
        c += 1
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, rv_life[0], header=3)
        ### dropping the unnamed column
        # sheet1.drop("Unnamed: 1",axis=1,inplace=True)
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet1["years"] = years
        ### concatinating the data sheets
        rev_20_17 = pd.concat([rev_20_17, sheet1])
    rev_20_17.set_index("Company", inplace=True)

    rev_20_17.index = company_renamer_life(rev_20_17.index)

    rev_20_17 = rev_20_17.loc["INSURERS":"REINSURERS", ]

    rev_20_17 = rev_20_17.loc["INSURERS":"TOTAL", ]

    rev_20_17.reset_index(inplace=True)

    rev_20_17.columns = col_renamer_life(rev_20_17.columns)

    ### BUSINESS IN FORCE
    years = year
    files = [files[0]]
    BIN_gen_19_17 = pd.DataFrame()
    c = 0
    for fil in files:
        years = years
        c += 1
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, bif_life[0], header=4)
        ### dropping the unnamed column
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet1["years"] = years
        ### concatinating the data sheets
        BIN_gen_19_17 = pd.concat([BIN_gen_19_17, sheet1])

    col_names = ["COMPANY"] + ["BUSINESS_IN_FORCE_New_" + i for i in BIN_gen_19_17.columns[1:4]] + [
        "BUSINESS_IN_FORCE_Total_" + i for i in BIN_gen_19_17.columns[4:7]] + ["YEARS"]

    BIN_gen_19_17.columns = col_names

    BIN_gen_19_17.columns = BIN_gen_19_17.columns.str.replace(".1", "")
    BIN_gen_19_17.columns = BIN_gen_19_17.columns.str.replace("*", "")
    BIN_gen_19_17.columns = col_renamer_life(BIN_gen_19_17.columns)

    BIN_gen_19_17.COMPANY = company_renamer(BIN_gen_19_17.COMPANY)

    # BIN_gen_19_17.set_index("COMPANY",inplace=True)

    # BIN_gen_19_17.index = company_renamer_life(BIN_gen_19_17.index)

    # BIN_gen_19_17=BIN_gen_19_17.loc[:"TOTAL",]

    # BIN_gen_19_17.reset_index(inplace=True)

    # BIN_gen_19_17.rename(columns={"index":"COMPANY"},inplace=True)

    #### APPENDIX 6
    years = year
    files = [files[0]]
    APP6_gen_19_17 = pd.DataFrame()
    c = 0
    for fil in files:
        years = years
        c += 1
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, app_6[0], header=3)
        ### dropping the unnamed column
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet1["years"] = years
        ### concatinating the data sheets
        APP6_gen_19_17 = pd.concat([APP6_gen_19_17, sheet1])

    APP6_gen_19_17.set_index("Company", inplace=True)

    APP6_gen_19_17.index = company_renamer_life(APP6_gen_19_17.index)

    APP6_gen_19_17 = APP6_gen_19_17.loc[:"REINSURERS", ]

    APP6_gen_19_17 = APP6_gen_19_17.loc[:"TOTAL", ]

    APP6_gen_19_17.reset_index(inplace=True)

    APP6_gen_19_17.columns = ["COMPANY"] + ["gross direct premiums_" + i for i in
                                            APP6_gen_19_17.columns[1:-1]] + ["YEARS"]

    APP6_gen_19_17.columns = col_renamer_life(APP6_gen_19_17.columns)

    ### APPENDIX 7

    years = year
    files = [files[0]]
    APP7_gen_19_17 = pd.DataFrame()
    c = 0
    for fil in files:
        years = years
        c += 1
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, app_7[0], header=3)
        ### dropping the unnamed column
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet1["years"] = years
        ### concatinating the data sheets
        APP7_gen_19_17 = pd.concat([APP7_gen_19_17, sheet1])

    APP7_gen_19_17.set_index("Company", inplace=True)

    APP7_gen_19_17.index = company_renamer_life(APP7_gen_19_17.index)

    APP7_gen_19_17.index.unique()

    APP7_gen_19_17 = APP7_gen_19_17.loc['INSURERS':'REINSURERS', ]

    APP7_gen_19_17 = APP7_gen_19_17.loc['INSURERS':'TOTAL', ]

    APP7_gen_19_17.reset_index(inplace=True)

    APP7_gen_19_17.columns = ["COMPANY"] + ["inward reinsurance premium_" + i for i in
                                            APP7_gen_19_17.columns[1:-1]] + ["YEARS"]

    APP7_gen_19_17.columns = col_renamer_life(APP7_gen_19_17.columns)

    ### APPENDIX 8

    years = year
    files = [files[0]]
    APP8_gen_19_17 = pd.DataFrame()
    c = 0
    for fil in files:
        years = years
        c += 1
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, app_8[0], header=3)
        ### dropping the unnamed column
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet1["years"] = years
        ### concatinating the data sheets
        APP8_gen_19_17 = pd.concat([APP8_gen_19_17, sheet1])

    APP8_gen_19_17.set_index("Company", inplace=True)

    APP8_gen_19_17.index = company_renamer_life(APP8_gen_19_17.index)

    APP8_gen_19_17 = APP8_gen_19_17.loc['INSURERS':'REINSURERS', ]

    APP8_gen_19_17 = APP8_gen_19_17.loc['INSURERS':'TOTAL', ]

    APP8_gen_19_17.reset_index(inplace=True)

    APP8_gen_19_17.columns = ["COMPANY"] + ["outward reinsurance premium_" + i for i in
                                            APP8_gen_19_17.columns[1:-1]] + ["YEARS"]

    APP8_gen_19_17.columns = col_renamer_life(APP8_gen_19_17.columns)

    #### APPENDIX 18

    years = year
    files = [files[0]]
    APP18_gen_19_17 = pd.DataFrame()
    c = 0
    for fil in files:
        years = years
        c += 1
        f1 = pd.ExcelFile(fil)
        ## reading the specific sheets
        sheet1 = pd.read_excel(f1, app_18[0], header=3)
        ### dropping the unnamed column
        sheet1.drop("Unnamed: 0", axis=1, inplace=True)
        sheet1["years"] = years
        ### concatinating the data sheets
        APP18_gen_19_17 = pd.concat([APP18_gen_19_17, sheet1])

    APP18_gen_19_17.set_index("Company", inplace=True)

    APP18_gen_19_17.index = company_renamer_life(APP18_gen_19_17.index)

    APP18_gen_19_17.index.unique()

    APP18_gen_19_17 = APP18_gen_19_17.loc['INSURERS':'REINSURERS', ]

    APP18_gen_19_17 = APP18_gen_19_17.loc['INSURERS':'TOTAL', ]

    APP18_gen_19_17.reset_index(inplace=True)

    APP18_gen_19_17.columns = ["COMPANY"] + ["acturial valuations_" + i for i in
                                             APP18_gen_19_17.columns[1:-1]] + ["YEARS"]

    APP18_gen_19_17.columns = col_renamer_life(APP18_gen_19_17.columns)

    #### accounting for old mutual and jubilee insurance life names

    names_OM = ["OLD MUTUAL", "OLD"]

    gen_df.loc[
        gen_df.COMPANY.str.contains('|'.join(names_OM), case=False), "COMPANY"] = "OLD_MUTUAL_LIFE"

    names_jub = ["jubilee", "JUBILEE"]

    BIN_gen_19_17.loc[
        BIN_gen_19_17.COMPANY.str.contains('|'.join(names_jub), case=False), "COMPANY"] = "JUBILEE-LIFE"

    rev_20_17.loc[
        rev_20_17.COMPANY.str.contains('|'.join(names_jub), case=False), "COMPANY"] = "JUBILEE-LIFE"

    APP6_gen_19_17.loc[APP6_gen_19_17.COMPANY.str.contains('|'.join(names_jub),
                                                           case=False), "COMPANY"] = "JUBILEE-LIFE"

    APP7_gen_19_17.loc[APP7_gen_19_17.COMPANY.str.contains('|'.join(names_jub),
                                                           case=False), "COMPANY"] = "JUBILEE-LIFE"

    APP8_gen_19_17.loc[APP8_gen_19_17.COMPANY.str.contains('|'.join(names_jub),
                                                           case=False), "COMPANY"] = "JUBILEE-LIFE"

    APP18_gen_19_17.loc[APP18_gen_19_17.COMPANY.str.contains('|'.join(names_jub),
                                                             case=False), "COMPANY"] = "JUBILEE-LIFE"

    gen_df.loc[gen_df.COMPANY.str.contains('|'.join(names_jub), case=False), "COMPANY"] = "JUBILEE-LIFE"

    ### merging
    life_21 = pd.merge(gen_df, rev_20_17, on=["COMPANY", "YEARS"])

    life_21 = pd.merge(life_21, BIN_gen_19_17, on=["COMPANY", "YEARS"])

    life_21 = pd.merge(life_21, APP6_gen_19_17, on=["COMPANY", "YEARS"])

    life_21 = pd.merge(life_21, APP7_gen_19_17, on=["COMPANY", "YEARS"])

    life_21 = pd.merge(life_21, APP8_gen_19_17, on=["COMPANY", "YEARS"])

    life_21 = pd.merge(life_21, APP18_gen_19_17, on=["COMPANY", "YEARS"])

    # life_21.to_csv(str(year)+"_life.csv")

    life_21.replace(
        {'ABSA_LIFE_KENYA': 'ABSA LIFE ASSURANCE KENYA', 'APA_LIFE': 'APA LIFE ASSURANCE COMPANY',
         'BARCLAYS_LIFE': 'BARCLAYS LIFE', 'BRITAM_LIFE': "BRITAM LIFE INSURANCE COMPANY",
         'CANNON': 'CANNON ASSURANCE COMPANY', 'CAPEX_LIFE': 'CAPEX LIFE ASSURANCE COMPANY',
         'CIC_LIFE': 'CIC LIFE ASSURANCE COMPANY', 'CORPORATE': 'CORPORATE INSURANCE COMPANY',
         'FIRST': 'FIRST ASSURANCE COMPANY',
         'GA_LIFE': 'GA LIFE ASSURANCE COMPANY', 'GEMINIA': 'GEMINIA INSURANCE COMPANY',
         'ICEA_LION_LIFE': 'ICEA LIFE ASSURANCE COMPANY', 'JUBILEE': 'JUBILEE INSURANCE COMPANY',
         'JUBILEE-LIFE': 'JUBILEE LIFE INSURANCE COMPANY',
         'KENINDIA': 'KENINDIA ASSURANCE COMPANY', 'KENYA_ORIENT_LIFE': 'KENYA ORIENT LIFE ASSURANCE',
         'KUSCCO_MUTUAL': 'KUSCO MUTUAL ASSURANCE LIMITED',
         'LIBERTY_LIFE': 'LIBERTY LIFE ASSURANCE COMPANY',
         'MADISON': 'MADISON INSURANCE COMPANY', 'METROPOLITAN_CANNON': 'METROPOLITAN CANNON INSURANCE',
         'OLD_MUTUAL_LIFE': 'OLD MUTUAL LIFE ASSURANCE', 'PIONEER': 'PIONEER ASSURANCE COMPANY',
         'PRUDENTIAL_LIFE': 'PRUDENTIAL LIFE ASSURANCE', 'SAHAM': 'SAHAM ASSURANCE',
         'SANLAM_LIFE': 'SANLAM LIFE INSURANCE',
         'TAKAFUL_OF_AFRICA': 'TAKAFUL OF AFRICA INSURANCE',
         'THE_KENYAN_ALLIANCE': 'THE KENYAN ALLIANCE INSURANCE',
         'THE_MONARCH': 'THE MONARCH INSURANCE', 'UAP_LIFE': 'UAP LIFE ASSURANCE COMPANY',
         'SHIELD_COMPANY': 'SHIELD ASSURANCE COMPANY', 'CFC_LIFE': 'CFC INSURANCE HOLDINGS',
         'CORPORATE_COMPANY': 'CORPORATE INSURANCE COMPANY',
         'MERCANTILE': 'MERCANTILE INSURANCE COMPANY LTD',
         'FIRST_COMPANY': 'FIRST ASSURANCE COMPANY', 'MADISON_COMPANY': 'MADISON INSURANCE COMPANY',
         'KENINDIA_COMPANY': 'KENINDIA ASSURANCE COMPANY',
         'GEMINIA_COMPANY': 'GEMINIA INSURANCE COMPANY', 'ABSA_LIFE': 'ABSA LIFE ASSURANCE KENYA',
         'JUBILEE_COMPANY': 'JUBILEE INSURANCE COMPANY',
         'CANNON_COMPANY': 'CANNON ASSURANCE COMPANY',
         'KUSCCO_MUTUAL_LIMITED': 'KUSCO MUTUAL ASSURANCE LIMITED',
         'PIONEER_COMPANY': 'PIONEER ASSURANCE COMPANY',
         'PAN_AFRICA_COMPANY': 'PAN AFRICA LIFE ASSURANCE COMPANY',
         'BRITISH_AMERICAN': "BRITISH AMERICAN INVESTMENTS COMPANY",
         'LIBERTY_LIFE_KENYA': 'LIBERTY LIFE ASSURANCE COMPANY',
         'OLD_MUTUAL': 'OLD MUTUAL LIFE ASSURANCE', 'PAN_AFRICA': 'PAN AFRICA LIFE ASSURANCE COMPANY',
         'SHIELD': 'SHIELD ASSURANCE COMPANY'}, inplace=True)



    ### original data for the database
    # LIFE
    if not table_exists(locals()['life_data']):
        life_data_full = pd.read_csv( os.path.join(APP_FILES_PATH,'original_files_dont_delete' ,"updated_life_data.csv"))
    else:
        life_data_full = pd.read_sql('SELECT * FROM ' + locals()["life_data"], engine)
        

   
    ### data addition after mining
    app_data_life = life_data_full

    growing_data_life = pd.concat([app_data_life, life_21])

    growing_data_life = growing_data_life.loc[:, ~growing_data_life.columns.str.contains('Unnamed')]
    growing_data_life = growing_data_life[~growing_data_life['COMPANY'].str.contains('TOTAL')]
    # growing_data_life = growing_data_life.apply(lambda x: round(x, 8) if x.dtype == 'float' else x)
    # growing_data_life.to_csv("C:\\Users\\Paul\\Desktop\\NCBA\\dir.csv")
    
    # try:
    #     # Begin a transaction
    #     with db_session.begin():
    #         # Your DataFrame to SQL operation
    #         growing_data_life.to_sql(locals()['life_data'], con=db_session.bind, if_exists='replace', index=False)

    #         # Other operations can go here
    # except Exception as e:
    #     db_session.rollback()  # Roll back on exception
    #     raise
    # finally:
    #     db_session.close()  # Ensure the session is closed when done


    growing_data_life.to_sql(locals()['life_data'], con=engine, if_exists='replace', index=False)




    data = growing_data_life

    return data





