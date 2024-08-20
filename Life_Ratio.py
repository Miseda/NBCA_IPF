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



def calculate_life_ratio(year, growing_data_life,table_names):
    for key, value in table_names.items():
        locals()[key] = value


    # calculate the ratio
    ratio = 0

    # Make Company and year columns to be the first and second columns

    def arrange_columns(df):
        a = df.columns.tolist()
        c = a.index("COMPANY")
        d = a[c:c + 1] + a[0:c] + a[c + 1:]
        b = d.index("YEARS")
        f = d[0:1] + d[b:b + 1] + d[1:b] + d[b + 1:]
        df = df.reindex(f, axis=1)
        return df

    # Remove commas assuming the first and second columns are company and year and the rest is numeric
    def remove_comma(df1):
        for i in df1.columns[2:]:
            df1[i] = df1[i].astype("string")
            df1[i] = df1[i].str.replace("\,", "")
            df1[i] = df1[i].str.replace(",", "")
            df1[i] = df1[i].str.replace("-", "0")
        df1.iloc[:, 2:] = df1.iloc[:, 2:].astype(float)
        return df1
    life = growing_data_life.sort_values(['COMPANY', 'YEARS'])
    life = arrange_columns(life)
    life = remove_comma(life)
    life = life.reset_index()
    life = life.drop("index", axis=1)

    def SIZE(df):
        dff = df[['COMPANY', 'YEARS']]
        df['SIZE'] = np.where(df['TOTAL_ASSETS'] >= 40000000, 'Large Size',
                              np.where(df['TOTAL_ASSETS'] >= 10000000, 'Medium Size',
                                       np.where(df['TOTAL_ASSETS'] >= 100, 'Small Size',
                                                np.nan)))
        return df

    SIZE(life)

    def CHANGE_CAPITAL_EMPLOYED(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['TOTAL_EQUITY_t-1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        dff['LONGTERM_LIABILITIES_t-1'] = df.groupby(['COMPANY'])['LONGTERM_LIABILITIES'].shift(1)
        dff['change'] = df['TOTAL_EQUITY'].fillna(0) + df['LONGTERM_LIABILITIES'].fillna(0) \
                        - dff['LONGTERM_LIABILITIES_t-1'].fillna(0) - dff['TOTAL_EQUITY_t-1'].fillna(0)
        df['CHANGE_CAPITAL_EMPLOYED'] = np.where(
            (dff['change'].isnull()) | (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0) \
            | (dff['TOTAL_EQUITY_t-1'].isnull()), np.nan,
            dff['change'] / (dff['TOTAL_EQUITY_t-1'].fillna(0) \
                             + dff['LONGTERM_LIABILITIES_t-1'].fillna(0)))
        return df

    CHANGE_CAPITAL_EMPLOYED(life)

    life['FLAG_CHANGE_CAPITAL_EMPLOYED'] = np.where(
        (life['CHANGE_CAPITAL_EMPLOYED'].isnull()) | (life['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((life['CHANGE_CAPITAL_EMPLOYED'] >= 0.5) \
                 | (life['CHANGE_CAPITAL_EMPLOYED'] <= -0.10), 1, 0))

    def GROWTH_IN_EQUITY(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['TOTAL_EQUITY_t-1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        dff['change'] = df['TOTAL_EQUITY'].fillna(0) - dff['TOTAL_EQUITY_t-1'].fillna(0)
        df['GROWTH_IN_EQUITY'] = np.where((df['TOTAL_EQUITY'].isnull()) | (df['TOTAL_ASSETS'].isnull()) \
                                          | (df['TOTAL_ASSETS'] == 0)| (dff['TOTAL_EQUITY_t-1'] == 0)  \
                                          | (dff['TOTAL_EQUITY_t-1'].isnull()), \
                                          np.nan, dff['change'] / dff['TOTAL_EQUITY_t-1'])
        return df

    GROWTH_IN_EQUITY(life)

    life['FLAG_GROWTH_IN_EQUITY'] = np.where(
        (life['GROWTH_IN_EQUITY'].isnull()) | (life['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((life['GROWTH_IN_EQUITY'] >= 0.5) | (life['GROWTH_IN_EQUITY'] <= -0.10),
                 1, 0))

    # NET_INCOME
    def NET_PROFIT_MARGIN(df):
        df['NET_PROFIT_MARGIN'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TRANSFER_TO_(FROM)_P_&_L'].isnull() \
                                             | (df['TOTAL_ASSETS'] == 0)), np.nan,
            df['TRANSFER_TO_(FROM)_P_&_L'] \
            / (df['NET_PREMIUM'].fillna(0) + df['TOTAL_BENEFITS'].fillna(0)))
        return df

    NET_PROFIT_MARGIN(life)

    life['FLAG_NET_PROFIT_MARGIN'] = np.where(
        (life['NET_PROFIT_MARGIN'].isnull()) | (life['TOTAL_ASSETS'].isnull()), np.nan,
        np.where(life['NET_PROFIT_MARGIN'] < 0, 1, 0))

    # Diversification (HHI)
    def DIVERSIFICATION(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['numerator'] = df['GROSS_DIRECT_PREMIUMS_LIFE_ASSURANCES'].fillna(0) ** 2 \
                           + df['GROSS_DIRECT_PREMIUMS_ANNUITIES'].fillna(0) ** 2 + df[
                               'GROSS_DIRECT_PREMIUMS_PENSIONS'].fillna(0) ** 2 \
                           + df['GROSS_DIRECT_PREMIUMS_GROUP_LIFE'].fillna(0) ** 2 \
                           + df['GROSS_DIRECT_PREMIUMS_GROUP_CREDIT'].fillna(0) ** 2 \
                           + df['GROSS_DIRECT_PREMIUMS_PERMANENT_HEALTH'].fillna(0) ** 2 \
                           + df['GROSS_DIRECT_PREMIUMS_INVESTMENTS'].fillna(0) ** 2
        df['DIVERSIFICATION'] = np.where(
            (df['GROSS_DIRECT_PREMIUMS_TOTAL'].isnull()) | (df['TOTAL_ASSETS'].isnull()), \
            np.nan, dff['numerator'] / df['GROSS_DIRECT_PREMIUMS_TOTAL'] ** 2)
        return df

    DIVERSIFICATION(life)

    life['FLAG_DIVERSIFICATION'] = np.where(
        (life['DIVERSIFICATION'].isnull()) | (life['TOTAL_ASSETS'].isnull()), np.nan,
        np.where(life['DIVERSIFICATION'] >= 0.5, 1, 0))

    def GROWTH_IN_PREMIUMS(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['GROSS_DIRECT_PREMIUMS_TOTAL_t-1'] = df.groupby(['COMPANY'])[
            'GROSS_DIRECT_PREMIUMS_TOTAL'].shift(1)
        dff['change'] = df['GROSS_DIRECT_PREMIUMS_TOTAL'].fillna(0) - dff[
            'GROSS_DIRECT_PREMIUMS_TOTAL_t-1'].fillna(0)
        df['GROWTH_IN_PREMIUMS'] = np.where(
            (df['GROSS_DIRECT_PREMIUMS_TOTAL'].isnull()) | (df['TOTAL_ASSETS'].isnull()) \
            | (df['TOTAL_ASSETS'] == 0) \
            | (dff['GROSS_DIRECT_PREMIUMS_TOTAL_t-1'].isnull()), \
            np.nan, dff['change'] / dff['GROSS_DIRECT_PREMIUMS_TOTAL_t-1'])
        return df

    GROWTH_IN_PREMIUMS(life)

    life['FLAG_GROWTH_IN_PREMIUMS'] = np.where(
        (life['GROWTH_IN_PREMIUMS'].isnull()) | (life['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((life['GROWTH_IN_PREMIUMS'] >= 0.33) | (life['GROWTH_IN_PREMIUMS'] <= -0.33),
                 1, 0))

    def NON_CURRENT_TO_CURRENT_ASSETS(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['CURRENT_ASSETS'] = df[
            ['PREFERENCE_SHARES_QUOTED', 'COMMERCIAL_PAPERS', 'DEBENTURES', 'ORDINARY_SHARES_QUOTED',
             'LOANS_SECURED_&_UNSECURED', 'MORTGAGES', 'TERM_DEPOSITS', 'CASH_AND_CASH_BALANCES',
             'OUTSTANDING_PREMIUMS', 'OTHER_RECEIVABLES', 'CORPORATE_BONDS', 'OTHER_SECURITIES',
             'GOVERNMENT_SECURITIES']].sum(axis=1)
        dff['NON_CURRENT_ASSETS'] = df[
            ['LAND_AND_BUILDINGS', 'INVESTMENT_PROPERTY', 'OTHER_FIXED_ASSETS',
             'INVESTMENT_IN_RELATED_COMPANIES', 'PREFERENCE_SHARES_UNQUOTED',
             'ORDINARY_SHARES_UNQUOTED', 'OTHER_ASSETS', 'INTANGIBLE_ASSETS']].sum(axis=1)

        dff['TOTAL_EQUITY_t-1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        dff['change'] = df['TOTAL_EQUITY'] - dff['TOTAL_EQUITY_t-1']
        df['NON_CURRENT_TO_CURRENT_ASSETS'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0) \
            | (dff['CURRENT_ASSETS'].isnull()) | (dff['NON_CURRENT_ASSETS'].isnull()),
            np.nan, dff['NON_CURRENT_ASSETS'] / dff['CURRENT_ASSETS'])
        return df

    NON_CURRENT_TO_CURRENT_ASSETS(life)

    life['FLAG_NON_CURRENT_TO_CURRENT_ASSETS'] = np.where(
        (life['NON_CURRENT_TO_CURRENT_ASSETS'].isnull()) \
        | (life['TOTAL_ASSETS'].isnull()), np.nan, \
        np.where((life['NON_CURRENT_TO_CURRENT_ASSETS'] >= 0.1) \
                 | (life['NON_CURRENT_TO_CURRENT_ASSETS'] <= -0.10), 1, 0))

    def REAL_ESTATE_TO_INVESTED_ASSETS(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['REAL_ESTATE_&_MORTGAGE'] = df[
            ['LAND_AND_BUILDINGS', 'INVESTMENT_PROPERTY', 'MORTGAGES']].sum(axis=1)
        dff['CASH_&_INVESTED_ASSETS'] = df[
            ['GOVERNMENT_SECURITIES', 'OTHER_SECURITIES', 'INVESTMENT_IN_RELATED_COMPANIES',
             'CORPORATE_BONDS', 'COMMERCIAL_PAPERS', 'DEBENTURES', 'ORDINARY_SHARES_QUOTED',
             'ORDINARY_SHARES_UNQUOTED', 'PREFERENCE_SHARES_QUOTED', 'PREFERENCE_SHARES_UNQUOTED',
             'LOANS_SECURED_&_UNSECURED', 'MORTGAGES', 'TERM_DEPOSITS', 'CASH_AND_CASH_BALANCES',
             'OUTSTANDING_PREMIUMS', 'OTHER_RECEIVABLES']].sum(axis=1)

        df['REAL_ESTATE_TO_INVESTED_ASSETS'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0) \
            | (dff['CASH_&_INVESTED_ASSETS'].isnull()) | (dff['REAL_ESTATE_&_MORTGAGE'] \
                                                          .isnull()),
            np.nan, dff['REAL_ESTATE_&_MORTGAGE'] / dff['CASH_&_INVESTED_ASSETS'])
        return df

    REAL_ESTATE_TO_INVESTED_ASSETS(life)

    life['FLAG_REAL_ESTATE_TO_INVESTED_ASSETS'] = np.where(
        (life['REAL_ESTATE_TO_INVESTED_ASSETS'].isnull()) | (life['TOTAL_ASSETS'] \
                                                             .isnull()), np.nan,
        np.where((life['REAL_ESTATE_TO_INVESTED_ASSETS'] >= 0.3) \
                 | (life['REAL_ESTATE_TO_INVESTED_ASSETS'] <= -0.30), 1, 0))

    def INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['CAPITAL_EMPLOYED'] = df['LONGTERM_LIABILITIES'].fillna(0) + df['TOTAL_EQUITY'].fillna(0)
        df['INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED'] = np.where(
            (df['INVESTMENT_IN_RELATED_COMPANIES'].isnull()) \
            | (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), np.nan, \
            df['INVESTMENT_IN_RELATED_COMPANIES'] / dff['CAPITAL_EMPLOYED'])
        return df

    INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED(life)

    life['FLAG_INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED'] = np.where(
        (life['INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED'] \
         .isnull()) | (life['TOTAL_ASSETS'].isnull()), np.nan, \
        np.where((life['INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED'] >= 1) \
                 | (life['INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED'] <= -1), 1, 0))

    def CHANGE_IN_ASSET_MIX(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['TOTAL'] = df[
            ['LAND_AND_BUILDINGS', 'INVESTMENT_PROPERTY', 'OTHER_FIXED_ASSETS', 'GOVERNMENT_SECURITIES',
             'OTHER_SECURITIES', 'INVESTMENT_IN_RELATED_COMPANIES', 'CORPORATE_BONDS',
             'COMMERCIAL_PAPERS',
             'DEBENTURES', 'ORDINARY_SHARES_UNQUOTED', 'ORDINARY_SHARES_QUOTED',
             'PREFERENCE_SHARES_QUOTED',
             'PREFERENCE_SHARES_UNQUOTED', 'LOANS_SECURED_&_UNSECURED', 'MORTGAGES', 'TERM_DEPOSITS',
             'CASH_AND_CASH_BALANCES', 'OUTSTANDING_PREMIUMS', 'OTHER_RECEIVABLES', 'OTHER_ASSETS',
             'INTANGIBLE_ASSETS']].sum(axis=1)
        dff['TOTAL_t_1'] = dff.groupby(['COMPANY'])['TOTAL'].shift()
        dff['LAND_AND_BUILDINGS'] = abs((df['LAND_AND_BUILDINGS'] / dff["TOTAL"]).fillna(0) \
                                        - ((df.groupby(['COMPANY'])['LAND_AND_BUILDINGS'].shift()) /
                                           dff['TOTAL_t_1']).fillna(0))
        dff['INVESTMENT_PROPERTY'] = abs((df['INVESTMENT_PROPERTY'] / dff["TOTAL"]).fillna(0) \
                                         - ((df.groupby(['COMPANY'])['INVESTMENT_PROPERTY'].shift()) /
                                            dff['TOTAL_t_1']).fillna(0))
        dff['OTHER_FIXED_ASSETS'] = abs((df['OTHER_FIXED_ASSETS'] / dff["TOTAL"]).fillna(0) \
                                        - ((df.groupby(['COMPANY'])['OTHER_FIXED_ASSETS'].shift()) /
                                           dff['TOTAL_t_1']).fillna(0))

        dff['GOVERNMENT_SECURITIES'] = abs((df['GOVERNMENT_SECURITIES'] / dff["TOTAL"]).fillna(0) \
                                           - ((df.groupby(['COMPANY'])[
                                                   'GOVERNMENT_SECURITIES'].shift()) / dff[
                                                  'TOTAL_t_1']).fillna(0))

        dff['OTHER_SECURITIES'] = abs((df['OTHER_SECURITIES'] / dff["TOTAL"]).fillna(0) \
                                      - ((df.groupby(['COMPANY'])['OTHER_SECURITIES'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['INVESTMENT_IN_RELATED_COMPANIES'] = abs(
            (df['INVESTMENT_IN_RELATED_COMPANIES'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['INVESTMENT_IN_RELATED_COMPANIES'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))

        dff['CORPORATE_BONDS'] = abs((df['CORPORATE_BONDS'] / dff["TOTAL"]).fillna(0) \
                                     - ((df.groupby(['COMPANY'])['CORPORATE_BONDS'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['COMMERCIAL_PAPERS'] = abs((df['COMMERCIAL_PAPERS'] / dff["TOTAL"]).fillna(0) \
                                       - ((df.groupby(['COMPANY'])['COMMERCIAL_PAPERS'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['DEBENTURES'] = abs((df['DEBENTURES'] / dff["TOTAL"]).fillna(0) \
                                - ((df.groupby(['COMPANY'])['DEBENTURES'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['ORDINARY_SHARES_QUOTED'] = abs((df['ORDINARY_SHARES_QUOTED'] / dff["TOTAL"]).fillna(0) \
                                            - ((df.groupby(['COMPANY'])[
                                                    'ORDINARY_SHARES_QUOTED'].shift()) / dff[
                                                   'TOTAL_t_1']).fillna(0))

        dff['ORDINARY_SHARES_UNQUOTED'] = abs((df['ORDINARY_SHARES_UNQUOTED'] / dff["TOTAL"]).fillna(0) \
                                              - ((df.groupby(['COMPANY'])[
                                                      'ORDINARY_SHARES_UNQUOTED'].shift()) / dff[
                                                     'TOTAL_t_1']).fillna(0))

        dff['PREFERENCE_SHARES_QUOTED'] = abs((df['PREFERENCE_SHARES_QUOTED'] / dff["TOTAL"]).fillna(0) \
                                              - ((df.groupby(['COMPANY'])[
                                                      'PREFERENCE_SHARES_QUOTED'].shift()) / dff[
                                                     'TOTAL_t_1']).fillna(0))

        dff['PREFERENCE_SHARES_UNQUOTED'] = abs(
            (df['PREFERENCE_SHARES_UNQUOTED'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['PREFERENCE_SHARES_UNQUOTED'].shift()) / dff[
                'TOTAL_t_1']).fillna(0))

        dff['LOANS_SECURED_&_UNSECURED'] = abs(
            (df['LOANS_SECURED_&_UNSECURED'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['LOANS_SECURED_&_UNSECURED'].shift()) / dff[
                'TOTAL_t_1']).fillna(0))

        dff['MORTGAGES'] = abs((df['MORTGAGES'] / dff["TOTAL"]).fillna(0) \
                               - ((df.groupby(['COMPANY'])['MORTGAGES'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['TERM_DEPOSITS'] = abs((df['TERM_DEPOSITS'] / dff["TOTAL"]).fillna(0) \
                                   - ((df.groupby(['COMPANY'])['TERM_DEPOSITS'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['CASH_AND_CASH_BALANCES'] = abs((df['CASH_AND_CASH_BALANCES'] / dff["TOTAL"]).fillna(0) \
                                            - ((df.groupby(['COMPANY'])[
                                                    'CASH_AND_CASH_BALANCES'].shift()) / dff[
                                                   'TOTAL_t_1']).fillna(0))

        dff['OUTSTANDING_PREMIUMS'] = abs((df['OUTSTANDING_PREMIUMS'] / dff["TOTAL"]).fillna(0) \
                                          - ((df.groupby(['COMPANY'])['OUTSTANDING_PREMIUMS'].shift()) /
                                             dff['TOTAL_t_1']).fillna(0))
        dff['OTHER_RECEIVABLES'] = abs((df['OTHER_RECEIVABLES'] / dff["TOTAL"]).fillna(0) \
                                       - ((df.groupby(['COMPANY'])['OTHER_RECEIVABLES'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['OTHER_ASSETS'] = abs((df['OTHER_ASSETS'] / dff["TOTAL"]).fillna(0) \
                                  - ((df.groupby(['COMPANY'])['OTHER_ASSETS'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        dff['INTANGIBLE_ASSETS'] = abs((df['INTANGIBLE_ASSETS'] / dff["TOTAL"]).fillna(0) \
                                       - ((df.groupby(['COMPANY'])['INTANGIBLE_ASSETS'].shift()) / dff[
            'TOTAL_t_1']).fillna(0))

        df['CHANGE_IN_ASSET_MIX'] = dff[['LAND_AND_BUILDINGS', 'INVESTMENT_PROPERTY',
                                         'OTHER_FIXED_ASSETS', 'GOVERNMENT_SECURITIES',
                                         'OTHER_SECURITIES', 'INVESTMENT_IN_RELATED_COMPANIES',
                                         'CORPORATE_BONDS', 'COMMERCIAL_PAPERS',
                                         'DEBENTURES', 'ORDINARY_SHARES_UNQUOTED',
                                         'ORDINARY_SHARES_QUOTED', 'PREFERENCE_SHARES_QUOTED',
                                         'PREFERENCE_SHARES_UNQUOTED', 'LOANS_SECURED_&_UNSECURED',
                                         'MORTGAGES', 'TERM_DEPOSITS',
                                         'CASH_AND_CASH_BALANCES', 'OUTSTANDING_PREMIUMS',
                                         'OTHER_RECEIVABLES', 'OTHER_ASSETS',
                                         'INTANGIBLE_ASSETS']].sum(axis=1) / 21
        return df

    CHANGE_IN_ASSET_MIX(life)

    life['FLAG_CHANGE_IN_ASSET_MIX'] = np.where(
        (life['CHANGE_IN_ASSET_MIX'].isnull()) | (life['TOTAL_ASSETS'].isnull()) \
        | (life['TOTAL_ASSETS'] == 0), np.nan, \
        np.where((life['CHANGE_IN_ASSET_MIX'] >= 0.05) \
                 | (life['CHANGE_IN_ASSET_MIX'] <= -0.05), 1, 0))

    def CHANGE_IN_PRODUCT_MIX(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['TOTAL'] = df[['GROSS_DIRECT_PREMIUMS_LIFE_ASSURANCES', 'GROSS_DIRECT_PREMIUMS_ANNUITIES',
                           'GROSS_DIRECT_PREMIUMS_GROUP_LIFE', 'GROSS_DIRECT_PREMIUMS_GROUP_CREDIT',
                           'GROSS_DIRECT_PREMIUMS_PERMANENT_HEALTH',
                           'GROSS_DIRECT_PREMIUMS_INVESTMENTS']].sum(axis=1)
        dff['TOTAL_t_1'] = dff.groupby(['COMPANY'])['TOTAL'].shift()
        dff['GROSS_DIRECT_PREMIUMS_LIFE_ASSURANCES'] = abs(
            (df['GROSS_DIRECT_PREMIUMS_LIFE_ASSURANCES'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['GROSS_DIRECT_PREMIUMS_LIFE_ASSURANCES'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))
        dff['GROSS_DIRECT_PREMIUMS_ANNUITIES'] = abs(
            (df['GROSS_DIRECT_PREMIUMS_ANNUITIES'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['GROSS_DIRECT_PREMIUMS_ANNUITIES'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))
        dff['GROSS_DIRECT_PREMIUMS_GROUP_LIFE'] = abs(
            (df['GROSS_DIRECT_PREMIUMS_GROUP_LIFE'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['GROSS_DIRECT_PREMIUMS_GROUP_LIFE'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))

        dff['GROSS_DIRECT_PREMIUMS_GROUP_CREDIT'] = abs(
            (df['GROSS_DIRECT_PREMIUMS_GROUP_CREDIT'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['GROSS_DIRECT_PREMIUMS_GROUP_CREDIT'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))

        dff['GROSS_DIRECT_PREMIUMS_PERMANENT_HEALTH'] = abs(
            (df['GROSS_DIRECT_PREMIUMS_PERMANENT_HEALTH'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['GROSS_DIRECT_PREMIUMS_PERMANENT_HEALTH'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))

        dff['GROSS_DIRECT_PREMIUMS_INVESTMENTS'] = abs(
            (df['GROSS_DIRECT_PREMIUMS_INVESTMENTS'] / dff["TOTAL"]).fillna(0) \
            - ((df.groupby(['COMPANY'])['GROSS_DIRECT_PREMIUMS_INVESTMENTS'].shift()) \
               / dff['TOTAL_t_1']).fillna(0))

        df['CHANGE_IN_PRODUCT_MIX'] = dff[['GROSS_DIRECT_PREMIUMS_LIFE_ASSURANCES',
                                           'GROSS_DIRECT_PREMIUMS_ANNUITIES',
                                           'GROSS_DIRECT_PREMIUMS_GROUP_LIFE',
                                           'GROSS_DIRECT_PREMIUMS_GROUP_CREDIT',
                                           'GROSS_DIRECT_PREMIUMS_PERMANENT_HEALTH',
                                           'GROSS_DIRECT_PREMIUMS_INVESTMENTS']] \
                                          .sum(axis=1) / 6
        return df

    CHANGE_IN_PRODUCT_MIX(life)

    life['FLAG_CHANGE_IN_PRODUCT_MIX'] = np.where(
        (life['CHANGE_IN_PRODUCT_MIX'].isnull()) | (life['TOTAL_ASSETS'].isnull()) \
        | (life['TOTAL_ASSETS'] == 0), np.nan, \
        np.where((life['CHANGE_IN_PRODUCT_MIX'] >= 0.05) \
                 | (life['CHANGE_IN_PRODUCT_MIX'] <= -0.05), 1, 0))

    ##Calculate the flags and overall scores

    # YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY

    # -------------------------------------------------------------------------------------------------------------

    life['FLAG_TOTAL'] = life[
        ['FLAG_CHANGE_CAPITAL_EMPLOYED', 'FLAG_GROWTH_IN_EQUITY', 'FLAG_NET_PROFIT_MARGIN',
         'FLAG_DIVERSIFICATION', 'FLAG_GROWTH_IN_PREMIUMS', 'FLAG_NON_CURRENT_TO_CURRENT_ASSETS',
         'FLAG_REAL_ESTATE_TO_INVESTED_ASSETS', 'FLAG_INVESTMENT_IN_AFFILIATES_TO_CAPITAL_EMPLOYED',
         'FLAG_CHANGE_IN_ASSET_MIX', 'FLAG_CHANGE_IN_PRODUCT_MIX']].sum(axis=1)

    def SMOOTHING_SCORE(df):
        dff = df[["COMPANY", "YEARS"]]
        dff['FLAG_TOTAL'] = df['FLAG_TOTAL']
        Alpha5 = 0.5
        Alpha3 = 0.6
        dff['TOTAL_FLAGS_t_1'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(1)
        dff['TOTAL_FLAGS_t_2'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(2)
        dff['TOTAL_FLAGS_t_3'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(3)
        dff['TOTAL_FLAGS_t_4'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(4)

        dff['threeyr'] = Alpha3 * df['FLAG_TOTAL'].fillna(0) + dff['TOTAL_FLAGS_t_1'].fillna(
            0) * Alpha3 * (1 - Alpha3) ** 1 \
                         + dff['TOTAL_FLAGS_t_2'].fillna(0) * (1 - Alpha3) ** 2
        dff['fiveyr'] = Alpha5 * df['FLAG_TOTAL'].fillna(0) + dff['TOTAL_FLAGS_t_1'].fillna(
            0) * Alpha5 * (1 - Alpha5) ** 1 \
                        + dff['TOTAL_FLAGS_t_2'].fillna(0) * Alpha5 * (1 - Alpha5) ** 2 + dff[
                            'TOTAL_FLAGS_t_3'].fillna(0) * \
                        Alpha5 * (1 - Alpha5) ** 3 + dff['TOTAL_FLAGS_t_4'].fillna(0) * (
                                1 - Alpha3) ** 4

        df['FLAG_5YR_SMOOTHING'] = np.where((df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0),
                                            np.nan, dff['fiveyr'])
        df['FLAG_3YR_SMOOTHING'] = np.where((df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0),
                                            np.nan, dff['threeyr'])
        return df

    SMOOTHING_SCORE(life)

    def SMOOTHING_SCORE_ADJ(df):
        df = df.sort_values(['COMPANY', 'YEARS'])
        dff = df[["COMPANY", "YEARS"]]
        dff['FLAG_TOTAL'] = df['FLAG_TOTAL']
        Alpha5 = 0.5
        Alpha3 = 0.6
        dff['TOTAL_FLAGS_t_1'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(1)
        dff['TOTAL_FLAGS_t_2'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(2)
        dff['TOTAL_FLAGS_t_3'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(3)
        dff['TOTAL_FLAGS_t_4'] = dff.groupby(['COMPANY'])['FLAG_TOTAL'].shift(4)

        dff['threeyr'] = Alpha3 * df['FLAG_TOTAL'].fillna(0) + dff['TOTAL_FLAGS_t_1'].fillna(
            0) * Alpha3 * (1 - Alpha3) ** 1 \
                         + dff['TOTAL_FLAGS_t_2'].fillna(0) * (1 - Alpha3) ** 2
        dff['fiveyr'] = Alpha5 * df['FLAG_TOTAL'].fillna(0) + dff['TOTAL_FLAGS_t_1'].fillna(
            0) * Alpha5 * (1 - Alpha5) ** 1 \
                        + dff['TOTAL_FLAGS_t_2'].fillna(0) * Alpha5 * (1 - Alpha5) ** 2 + dff[
                            'TOTAL_FLAGS_t_3'].fillna(0) * \
                        Alpha5 * (1 - Alpha5) ** 3 + dff['TOTAL_FLAGS_t_4'].fillna(0) * (
                                1 - Alpha3) ** 4

        dff['FLAG_5YR_SMOOTHING_ADJ'] = np.where(df['SIZE'] == 'Large Size', dff['fiveyr'] * 0.8,  # TA
                                                 np.where(df['SIZE'] == 'Small Size',
                                                          dff['fiveyr'] * 1.15,
                                                          dff['fiveyr']))

        dff['FLAG_3YR_SMOOTHING_ADJ'] = np.where(df['SIZE'] == 'Large Size', dff['threeyr'] * 0.8,  # TA
                                                 np.where(df['SIZE'] == 'Small Size',
                                                          dff['threeyr'] * 1.15,
                                                          dff['threeyr']))

        df['FLAG_3YR_SMOOTHING_ADJ'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), np.nan,
            dff['FLAG_3YR_SMOOTHING_ADJ'])

        df['FLAG_5YR_SMOOTHING_ADJ'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), np.nan,
            dff['FLAG_5YR_SMOOTHING_ADJ'])
        return df

    life = SMOOTHING_SCORE_ADJ(life)

    life['INSURANCE_SCORE'] = life['FLAG_3YR_SMOOTHING_ADJ']

    life['RATING'] = np.where((life['INSURANCE_SCORE'].isnull()) | (life['TOTAL_ASSETS'].isnull()),
                              np.nan, \
                              np.where(life['INSURANCE_SCORE'] > 5, "D",
                                       np.where(life['INSURANCE_SCORE'] > 3.5, "C",
                                                np.where(life['INSURANCE_SCORE'] > 2, "B", "A"))))

    ####### saving the resuting files in a results folder

    
    # result_path = os.path.join(APP_FILES_PATH, "Resulting_Files")

    # Create the result folder if it doesn't exist
    
    
    # if not os.path.exists(result_path):
    #     os.makedirs(result_path)


    # file_name_life = str(year) + "_life.csv"

    # file_path_life = os.path.join(result_path, file_name_life)

    life = life.loc[:, ~life.columns.str.contains('Unnamed')]
    life = life.apply(lambda x: round(x, 8) if x.dtype == 'float' else x)
    life.replace([np.inf, -np.inf], np.nan, inplace=True)
    # life.to_csv("C:\\Users\\Paul\\Desktop\\NCBA\\afterscores.csv")
    # try:
    #     # Begin a transaction
    #     with db_session.begin():
    #         # Your DataFrame to SQL operation
    #         life.to_sql(locals()['life_ratio'], con=db_session.bind, if_exists='replace', index=False)
    #         # Other operations can go here
    # except Exception as e:
    #     db_session.rollback()  # Roll back on exception
    #     raise
    # finally:
    #     db_session.close()  # Ensure the session is closed when done

    
    life.to_sql(locals()['life_ratio'], con=engine, if_exists='replace', index=False)

   
    

    # life_final.to_csv(file_path_life)
    ratio= life

    return ratio