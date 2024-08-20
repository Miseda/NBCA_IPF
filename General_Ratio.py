import csv
from urllib.parse import quote_plus
import warnings
from csv import writer
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, Table, MetaData
from sqlalchemy.orm import sessionmaker

load_dotenv() # specify the path to the .env file
APP_FILES_PATH = os.environ.get('APP_FILES_PATH')



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


def calculate_general_ratio( year, growing_data,table_names):
    # calculate the life ratio
    for key, value in table_names.items():
        locals()[key] = value


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
    def clean_dat(value):
        if isinstance(value, str):
            # Replace backslash-comma and comma
            value = value.replace('\\,', '').replace(',', '')

            # Replace standalone hyphen with zero
            if value == '-':
                value = '0'

            # Replace hyphens not part of negative numbers
            elif '-' in value and not value.startswith('-'):
                value = value.replace('-', '0')
        return value

    def remove_comma(df1):
        df1[df1.columns[2:]].applymap(clean_dat)
        for i in df1.columns[2:]:
            df1[i] = df1[i].astype("string")
            df1[i] = df1[i].str.replace("\,", "")
            df1[i] = df1[i].str.replace(",", "")
        df1.iloc[:, 2:] = df1.iloc[:, 2:].astype(float)
        return df1

    df = growing_data

    # df.to_csv(r"C:\work stuff\Projects\Credit Scoring Project\Insurance-Credit-Scoring\Data\raw_data\file.csv")
    # Make company and Years, the first two columns
    # Sort all values by Company then by year
    # Remove commas
    df = pd.DataFrame(df)
    df = df.sort_values(['COMPANY', 'YEARS'])
    df = arrange_columns(df)
    df = remove_comma(df)
    df = df.reset_index()
    df = df.drop("index", axis=1)

    #### company size partitions
    def SIZE(df):
        dff = df[['COMPANY', 'YEARS']]
        df['SIZE'] = np.where(df['TOTAL_ASSETS'] >= 10000000, 'Large Size',
                              np.where(df['TOTAL_ASSETS'] >= 4000000, 'Medium Size',
                                       np.where(df['TOTAL_ASSETS'] >= 100, 'Small Size',
                                                np.nan)))
        return df

    SIZE(df)

    ##### ratios

    # Gross Premium Written to Equity (GPW_E)
    def GROSS_PREMIUM_WRITTEN_TO_EQUITY_RATIO(df):
        df['GROSS_PREMIUM_TO_EQUITY'] = np.where((df['GROSS_DIRECT_PREMIUM'].isnull()) \
                                                 | (df['TOTAL_ASSETS'].isnull()) | (
                                                         df['TOTAL_EQUITY'].isnull() \
                                                         | (df['TOTAL_ASSETS'] == 0)),
                                                 np.nan,
                                                 df['GROSS_DIRECT_PREMIUM'] / df['TOTAL_EQUITY'])
        return df

    GROSS_PREMIUM_WRITTEN_TO_EQUITY_RATIO(df)

    # Gross Premium Written to Equity (GPW_E)
    def NET_PREMIUM_TO_EQUITY(df):
        df['NET_PREMIUM_TO_EQUITY'] = np.where((df['NET_PREMIUM_WRITTEN'].isnull()) \
                                               | (df['TOTAL_ASSETS'].isnull()) | (
                                                       df['TOTAL_EQUITY'].isnull() \
                                                       | (df['TOTAL_ASSETS'] == 0)),
                                               np.nan, df['NET_PREMIUM_WRITTEN'] / df['TOTAL_EQUITY'])
        return df

    NET_PREMIUM_TO_EQUITY(df)

    # NET_PREMIUM_GROWTH_RATIO
    def NET_PREMIUM_GROWTH_RATIO(df):
        dff = df[['COMPANY', 'YEARS']]
        net_premium_written_0 = df['NET_PREMIUM_WRITTEN']
        dff['net_premium_written_1'] = df.groupby(['COMPANY'])['NET_PREMIUM_WRITTEN'].shift(1)
        dff['difference'] = df['NET_PREMIUM_WRITTEN'].fillna(0) - dff['net_premium_written_1'].fillna(0)
        df['NET_PREMIUM_GROWTH'] = np.where(
            (df['NET_PREMIUM_WRITTEN'].isnull()) | (dff['net_premium_written_1'].isnull()) \
            | (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0),
            np.nan, dff['difference'] / dff['net_premium_written_1'])
        return df

    NET_PREMIUM_GROWTH_RATIO(df)

    # Premium Ceded to Equity (PC_E)
    def PREMIUM_CEDED_TO_EQUITY(df):
        dff = df[['COMPANY', 'YEARS']]
        dff['ceded'] = df['OUTWARD_REINSURANCE'].fillna(0) - df['INWARD_REINSURANCE'].fillna(0)
        df['PREMIUM_CEDED_TO_EQUITY'] = np.where((dff['ceded'].isnull()) \
                                                 | (df['TOTAL_ASSETS'].isnull()) | (
                                                         df['TOTAL_EQUITY'].isnull() \
                                                         | (df['TOTAL_ASSETS'] == 0)),
                                                 np.nan, dff['ceded'] / df['TOTAL_EQUITY'])
        return df

    PREMIUM_CEDED_TO_EQUITY(df)

    # Two Year Operating Ratio (2yrOR)

    def TWO_YEAR_OPERATING_RATIO(df):
        dff = df[['COMPANY', 'YEARS']]
        dff['net_premium_written_t_1'] = df.groupby(['COMPANY'])['NET_PREMIUM_WRITTEN'].shift(1)
        dff['net_earned_premium_t_1'] = df.groupby(['COMPANY'])['NET_EARNED_PREMIUM_INCOME'].shift(1)
        dff['net_commissions_t_1'] = df.groupby(['COMPANY'])['NET_COMMISIONS'].shift(1)
        dff['incurred_claims_t_1'] = df.groupby(['COMPANY'])['INCURRED_CLAIMS'].shift(1)
        dff['management_expenses_t_1'] = df.groupby(['COMPANY'])['EXPENSE_OF_MANAGEMENT'].shift(1)
        dff['investment_income_t_1'] = df.groupby(['COMPANY'])['INVESTMENT_INCOME'].shift(1)
        dff['expense_to_premium_wrtitten'] = (df['EXPENSE_OF_MANAGEMENT'].fillna(0) + df[
            'NET_COMMISIONS'].fillna(0) \
                                              + dff['net_commissions_t_1'].fillna(0) + dff[
                                                  'management_expenses_t_1'].fillna(0)) \
                                             / (df['NET_PREMIUM_WRITTEN'].fillna(0) + dff[
            'net_premium_written_t_1'].fillna(0))
        dff["investment_to_net_earned_premium"] = (df['INVESTMENT_INCOME'].fillna(0) + dff[
            'investment_income_t_1'].fillna(0)) \
                                                  / (df['NET_EARNED_PREMIUM_INCOME'].fillna(0) \
                                                     + dff['net_earned_premium_t_1'].fillna(0))
        dff["incurred_claims_to_net_earned_premium"] = (df['INCURRED_CLAIMS'].fillna(0) + dff[
            'incurred_claims_t_1'].fillna(0)) \
                                                       / (df['NET_EARNED_PREMIUM_INCOME'].fillna(0) \
                                                          + dff['net_earned_premium_t_1'].fillna(0))
        df['TWO_YEAR_OPERATING'] = np.where((df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0),
                                            np.nan, dff['expense_to_premium_wrtitten'].fillna(0) - \
                                            dff["investment_to_net_earned_premium"].fillna(0) \
                                            + dff["incurred_claims_to_net_earned_premium"].fillna(0))
        return df

    TWO_YEAR_OPERATING_RATIO(df)

    # Investment Yield Ratio
    def INVESTMENT_YIELD_RATIO(df):
        dff = df[['COMPANY', 'YEARS']]
        dff['investment'] = df[
            ['GOVERNMENT_SECURITIES', 'OTHER_SECURITIES', 'CORPORATE_BONDS', 'COMMERCIAL_PAPERS',
             'DEBENTURES',
             'ORDINARY_SHARES_QUOTED', 'ORDINARY_SHARES_UNQUOTED', 'PREFERENCE_SHARES_QUOTED',
             'PREFERENCE_SHARES_UNQUOTED', 'LOANS_SECURED_&_UNSECURED', 'MORTGAGES', 'TERM_DEPOSITS',
             'CASH_AND_CASH_BALANCES']].sum(axis=1)

        dff['investment_t-1'] = dff.groupby(['COMPANY'])['investment'].shift(1)
        dff['longterm_liabilities'] = df.groupby(['COMPANY'])['LONGTERM_LIABILITIES'].shift(1)
        dff['TWO_YR_NET_INVESTMENT'] = dff['investment'].fillna(0) + dff['investment_t-1'].fillna(0) \
                                       - df['LONGTERM_LIABILITIES'].fillna(0) - dff[
                                           'longterm_liabilities'].fillna(0)
        df['INVESTMENT_YIELD'] = np.where(
            (df['INVESTMENT_INCOME'].isnull()) | (dff['TWO_YR_NET_INVESTMENT'].isnull()) \
            | (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), \
            np.nan, (df['INVESTMENT_INCOME'] / dff['TWO_YR_NET_INVESTMENT']) * 2)
        return df

    INVESTMENT_YIELD_RATIO(df)

    # liquidity_ratio
    def LIQUIDITY_RATIO(df):
        dff = df[['COMPANY', 'YEARS']]
        dff['current_assets'] = df[
            ['TERM_DEPOSITS', 'CASH_AND_CASH_BALANCES', 'OUTSTANDING_PREMIUMS', 'OTHER_RECEIVABLES',
             'COMMERCIAL_PAPERS', 'GOVERNMENT_SECURITIES', 'OTHER_SECURITIES', 'OTHER_ASSETS',
             'ORDINARY_SHARES_QUOTED']].sum(axis=1)

        dff['current_liabilities'] = df[
            ['UNDERWRITING_PROVISIONS', 'CURRENT_LIABILITIES', 'ACTUARIAL_CONTRACT_LIABILITIES']] \
            .sum(axis=1)

        df["LIQUIDITY_RATIO"] = np.where((dff['current_assets'].isnull()) \
                                         | (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), \
                                         np.nan, dff['current_assets'] / dff['current_liabilities'])
        return df

    LIQUIDITY_RATIO(df)

    # Commissions to Equity
    def COMMISSIONS_TO_EQUITY(df):
        #     dff=df[['COMPANY','YEARS']]
        #     dff = dff.sort_values(['COMPANY','YEARS'])
        df['COMMISSIONS_TO_EQUITY'] = np.where(
            (df['NET_COMMISIONS'].isnull()) | (df['TOTAL_ASSETS'].isnull()), \
            np.nan, df['NET_COMMISIONS'] / df['TOTAL_EQUITY'])
        return df

    COMMISSIONS_TO_EQUITY(df)

    # One Year Change in Reserve to Equity (1yr CRE)

    def ONE_YEAR_CHANGE_RESERVE_EQUITY_RATIO(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['incurred_claim'] = df['INCURRED_CLAIMS']
        dff['incurred_claim_t_1'] = df.groupby(['COMPANY'])['INCURRED_CLAIMS'].shift(1)
        dff['net_paid_claims'] = df['NET_PAID_CLAIMS_TOTAL']
        dff['paid_claims_combined'] = np.where(df['NET_PAID_CLAIMS_TOTAL'].isnull(),
                                               dff['incurred_claim_t_1'], \
                                               df['NET_PAID_CLAIMS_TOTAL'])
        dff['reserve'] = dff['incurred_claim'].fillna(0) - dff['paid_claims_combined'].fillna(0)
        dff['total_equity_t_1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        df['ONE_YR_CHANGE_RESERVE_EQUITY'] = np.where(
            (dff['reserve'].isnull()) | (dff['total_equity_t_1'].isnull()) \
            | (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), np.nan, \
            dff['reserve'] / dff['total_equity_t_1'])
        return df

    ONE_YEAR_CHANGE_RESERVE_EQUITY_RATIO(df)

    # Two year Change in Reserve to Equity (1yr CRE)
    def TWO_YR_CHANGE_RESERVE_EQUITY(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['incurred_claim'] = df['INCURRED_CLAIMS']
        dff['incurred_claim_t_1'] = df.groupby(['COMPANY'])['INCURRED_CLAIMS'].shift(1)
        dff['incurred_claim_t_2'] = df.groupby(['COMPANY'])['INCURRED_CLAIMS'].shift(2)
        dff['net_paid_claims'] = df['NET_PAID_CLAIMS_TOTAL']
        dff['net_paid_claims_t_1'] = df.groupby(['COMPANY'])['NET_PAID_CLAIMS_TOTAL'].shift(1)
        dff['reserve1'] = dff['incurred_claim'].fillna(0) + dff['incurred_claim_t_1'].fillna(0) \
                          - dff['net_paid_claims'].fillna(0) - dff['net_paid_claims_t_1'].fillna(0)
        dff['reserve2'] = dff['incurred_claim'].fillna(0) - dff['incurred_claim_t_2'].fillna(0)
        dff['reserve_combined'] = np.where(
            (dff['net_paid_claims'].isnull()) | (dff['net_paid_claims_t_1'].isnull()), \
            dff['reserve2'], dff['reserve1'])
        dff['total_equity_t_1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        dff['total_equity_t_2'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(2)
        df['TWO_YR_CHANGE_RESERVE_EQUITY'] = np.where((dff['reserve_combined'].isnull()) \
                                                      | (dff['total_equity_t_2'].isnull()) | (
                                                          df['TOTAL_ASSETS'].isnull()) | (
                                                              df['TOTAL_ASSETS'] == 0), \
                                                      np.nan,
                                                      dff['reserve_combined'] / dff['total_equity_t_2'])

        return df

    TWO_YR_CHANGE_RESERVE_EQUITY(df)

    # Reserve Deficiency to Equity (RDE)

    def RESERVE_DEFICIENCY_EQUITY(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['incurred_claim'] = df['INCURRED_CLAIMS']
        dff['incurred_claim_t_1'] = df.groupby(['COMPANY'])['INCURRED_CLAIMS'].shift(1)
        dff['incurred_claim_t_2'] = df.groupby(['COMPANY'])['INCURRED_CLAIMS'].shift(2)
        dff['net_paid_claims'] = df['NET_PAID_CLAIMS_TOTAL']
        dff['net_paid_claims_t_1'] = df.groupby(['COMPANY'])['NET_PAID_CLAIMS_TOTAL'].shift(1)
        dff['reserve1'] = dff['incurred_claim'].fillna(0) + dff['incurred_claim_t_1'].fillna(0) \
                          - dff['net_paid_claims'].fillna(0) - dff['net_paid_claims_t_1'].fillna(0)
        dff['reserve2'] = dff['incurred_claim'].fillna(0) - dff['incurred_claim_t_2'].fillna(0)
        dff['two_year_reserve_development'] = np.where(
            (dff['net_paid_claims'].isnull()) | (dff['net_paid_claims_t_1'].isnull()), \
            dff['reserve2'], dff['reserve1'])
        dff['total_equity_t_1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        dff['total_equity_t_2'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(2)
        dff['paid_claims_combined'] = np.where(df['NET_PAID_CLAIMS_TOTAL'].isnull(),
                                               dff['incurred_claim_t_1'], \
                                               df['NET_PAID_CLAIMS_TOTAL'])
        dff['one_year_reserve_development'] = dff['incurred_claim'].fillna(0) - dff[
            'paid_claims_combined'].fillna(0)
        dff['NET_EARNED_PREMIUM_TOTAL_1'] = df.groupby(['COMPANY'])["NET_EARNED_PREMIUM_TOTAL"].shift(1)
        dff['NET_EARNED_PREMIUM_TOTALL_2'] = df.groupby(['COMPANY'])["NET_EARNED_PREMIUM_TOTAL"].shift(
            2)
        dff['actuarial_contract_liability_t_1'] = df.groupby(['COMPANY'])[
            'ACTUARIAL_CONTRACT_LIABILITIES'].shift(1)
        dff['actuarial_contract_liability_t_2'] = df.groupby(['COMPANY'])[
            'ACTUARIAL_CONTRACT_LIABILITIES'].shift(2)
        dff['TWO_ACTUARIAL_CONTRACT_LIABILITIES'] = dff['actuarial_contract_liability_t_2'].fillna(0) \
                                                    + dff['two_year_reserve_development'].fillna(0)
        dff['ONE_ACTUARIAL_CONTRACT_LIABILITIES'] = dff['actuarial_contract_liability_t_1'].fillna(0) \
                                                    + dff['one_year_reserve_development'].fillna(0)
        dff['2_ratio'] = dff['TWO_ACTUARIAL_CONTRACT_LIABILITIES'] / dff['NET_EARNED_PREMIUM_TOTALL_2']
        dff['1_ratio'] = dff['ONE_ACTUARIAL_CONTRACT_LIABILITIES'] / dff['NET_EARNED_PREMIUM_TOTAL_1']
        dff['average_ratio'] = (dff['1_ratio'].fillna(0) + dff['2_ratio'].fillna(0)) / 2
        dff['defficient_reserve'] = dff['average_ratio'] * df['NET_EARNED_PREMIUM_INCOME'] - df[
            'ACTUARIAL_CONTRACT_LIABILITIES']

        dff['score'] = np.where(
            (df['NET_EARNED_PREMIUM_INCOME'].isnull()) | (df['TOTAL_EQUITY'].isnull()) \
            | (dff['defficient_reserve'].isnull()) | (df['TOTAL_ASSETS'].isnull()) | (
                    df['TOTAL_ASSETS'] == 0),
            np.nan, dff['defficient_reserve'] / df['TOTAL_EQUITY'])

        dff['score1'] = np.where((dff['score'] > 0) & (df['TOTAL_EQUITY'] <= 0), 0.99, dff['score'])
        df['RESERVE_DEFICIENCY_EQUITY'] = np.where((dff['score1'] <= 0) & (df['TOTAL_EQUITY'] <= 0), 0,
                                                   dff['score1'])
        return df

    RESERVE_DEFICIENCY_EQUITY(df)

    # Diversification (HHI)
    def DIVERSIFICATION(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['numerator'] = df['NET_EARNED_PREMIUM_AVIATION'].fillna(0) ** 2 + df[
            'NET_EARNED_PREMIUM_ENGINEERING'].fillna(0) ** 2 \
                           + df['NET_EARNED_PREMIUM_FIRE_DOMESTIC'].fillna(0) ** 2 + df[
                               'NET_EARNED_PREMIUM_FIRE_INDUSTRIAL'].fillna(0) ** 2 \
                           + df['NET_EARNED_PREMIUM_LIABILITY'].fillna(0) ** 2 + df[
                               'NET_EARNED_PREMIUM_MARINE'].fillna(0) ** 2 \
                           + df['NET_EARNED_PREMIUM_MOTOR_PRIVATE'].fillna(0) ** 2 + df[
                               'NET_EARNED_PREMIUM_MOTOR_COMMERCIAL'].fillna(0) ** 2 \
                           + df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL_PSV'].fillna(0) ** 2 + df[
                               'NET_EARNED_PREMIUM_PERSONAL_ACCIDENT'].fillna(0) ** 2 \
                           + df['NET_EARNED_PREMIUM_THEFT'].fillna(0) ** 2 + df[
                               "NET_EARNED_PREMIUM_WORKMENS'_COMPENSATION"].fillna(0) ** 2 \
                           + df['NET_EARNED_PREMIUM_MEDICAL'].fillna(0) ** 2 + df[
                               'NET_EARNED_PREMIUM_MISCELLANEOUS'].fillna(0) ** 2
        df['DIVERSIFICATION'] = np.where(
            (df['NET_EARNED_PREMIUM_INCOME'].isnull()) | (df['TOTAL_ASSETS'].isnull()), \
            np.nan, dff['numerator'] / df['NET_EARNED_PREMIUM_TOTAL'] ** 2)
        return df

    DIVERSIFICATION(df)

    # Growth in Equity
    def GROWTH_IN_EQUITY(df):
        dff = df[['COMPANY', 'YEARS']]
        dff = dff.sort_values(['COMPANY', 'YEARS'])
        dff['TOTAL_EQUITY_t-1'] = df.groupby(['COMPANY'])['TOTAL_EQUITY'].shift(1)
        dff['change'] = df['TOTAL_EQUITY'].fillna(0) - dff['TOTAL_EQUITY_t-1'].fillna(0)
        df['GROWTH_IN_EQUITY'] = np.where((df['TOTAL_EQUITY'].isnull()) | (df['TOTAL_ASSETS'].isnull()) \
                                          | (df['TOTAL_ASSETS'] == 0) \
                                          | (dff['TOTAL_EQUITY_t-1'].isnull()), \
                                          np.nan, dff['change'] / dff['TOTAL_EQUITY_t-1'])
        return df

    GROWTH_IN_EQUITY(df)

    ##Calculate the flags and overall scores

    df['FLAG_COMMISSIONS_TO_EQUITY'] = np.where(
        (df['COMMISSIONS_TO_EQUITY'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where(df['COMMISSIONS_TO_EQUITY'] > 0.3, 1, 0))

    df['FLAG_DIVERSIFICATION'] = np.where(
        (df['DIVERSIFICATION'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where(df['DIVERSIFICATION'] >= 0.5, 1, 0))

    df['FLAG_GROSS_PREMIUM_TO_EQUITY'] = np.where(
        (df['GROSS_PREMIUM_TO_EQUITY'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((df['GROSS_PREMIUM_TO_EQUITY'] >= 5) | (df['GROSS_PREMIUM_TO_EQUITY'] < 0),
                 1, 0))

    df['FLAG_GROWTH_IN_EQUITY'] = np.where(
        (df['GROWTH_IN_EQUITY'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((df['GROWTH_IN_EQUITY'] >= 0.5) | (df['GROWTH_IN_EQUITY'] <= -0.15),
                 1, 0))

    df['FLAG_INVESTMENT_YIELD'] = np.where(
        (df['INVESTMENT_YIELD'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where(df['INVESTMENT_YIELD'] > 0.2, 1, 0))

    df['FLAG_LIQUIDITY_RATIO'] = np.where(
        (df['LIQUIDITY_RATIO'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((df['LIQUIDITY_RATIO'] < 0.6), 2, np.where((df['LIQUIDITY_RATIO'] < 1),
                                                            1, 0)))

    df['FLAG_NET_PREMIUM_GROWTH'] = np.where(
        (df['NET_PREMIUM_GROWTH'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((df['NET_PREMIUM_GROWTH'] >= 0.33) | (df['NET_PREMIUM_GROWTH'] <= -0.33),
                 1, 0))

    df['FLAG_NET_PREMIUM_TO_EQUITY'] = np.where(
        (df['NET_PREMIUM_TO_EQUITY'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where((df['YEARS'].astype(int) >= int(2020)) & (df['TOTAL_ASSETS'] <= 600000), 1,
                 np.where((df['YEARS'].astype(int) >= int(2020)) & (df['TOTAL_ASSETS'] < 600000), 1,
                          np.where(
                              (df['NET_PREMIUM_TO_EQUITY'] > 3) | (df['NET_PREMIUM_TO_EQUITY'] < 0),
                              1, 0))))

    df['FLAG_ONE_YR_CHANGE_RESERVE_EQUITY'] = np.where((df['ONE_YR_CHANGE_RESERVE_EQUITY'].isnull()) \
                                                       | (df['TOTAL_ASSETS'].isnull()), np.nan,
                                                       np.where(( \
                                                                            df[
                                                                                'ONE_YR_CHANGE_RESERVE_EQUITY'] > 0.2) | (
                                                                        df[
                                                                            'ONE_YR_CHANGE_RESERVE_EQUITY'] < -0.2),
                                                                1, 0))

    df['FLAG_PREMIUM_CEDED_TO_EQUITY'] = np.where((df['PREMIUM_CEDED_TO_EQUITY'].isnull()) \
                                                  | (df['TOTAL_ASSETS'].isnull()), np.nan, np.where(( \
                                                                                                                df[
                                                                                                                    'PREMIUM_CEDED_TO_EQUITY'] > 1) | (
                                                                                                            df[
                                                                                                                'PREMIUM_CEDED_TO_EQUITY'] < -1),
                                                                                                    1, \
                                                                                                    np.where(
                                                                                                        (
                                                                                                                df[
                                                                                                                    'PREMIUM_CEDED_TO_EQUITY'] > -0.1) & (
                                                                                                                df[
                                                                                                                    'PREMIUM_CEDED_TO_EQUITY'] < 0.1),
                                                                                                        1,
                                                                                                        0)))

    df['FLAG_RESERVE_DEFICIENCY_EQUITY'] = np.where((df['RESERVE_DEFICIENCY_EQUITY'].isnull()) \
                                                    | (df['TOTAL_ASSETS'].isnull()), np.nan, np.where(( \
                                                                                                                  df[
                                                                                                                      'RESERVE_DEFICIENCY_EQUITY'] > 0.25) | (
                                                                                                              df[
                                                                                                                  'RESERVE_DEFICIENCY_EQUITY'] < -0.25),
                                                                                                      1,
                                                                                                      0))

    df['FLAG_TWO_YR_CHANGE_RESERVE_EQUITY'] = np.where((df['TWO_YR_CHANGE_RESERVE_EQUITY'].isnull()) \
                                                       | (df['TOTAL_ASSETS'].isnull()), np.nan,
                                                       np.where(( \
                                                                            abs(df[
                                                                                    'TWO_YR_CHANGE_RESERVE_EQUITY']) > abs(
                                                                        df[
                                                                            'ONE_YR_CHANGE_RESERVE_EQUITY'])) \
                                                                & (df[
                                                                       'FLAG_ONE_YR_CHANGE_RESERVE_EQUITY'] == 1),
                                                                1, 0))

    df['FLAG_TWO_YEAR_OPERATING'] = np.where(
        (df['TWO_YEAR_OPERATING'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
        np.where(df['TWO_YEAR_OPERATING'] > 1, 1, 0))

    # YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY

    # -------------------------------------------------------------------------------------------------------------

    df['FLAG_TOTAL'] = df[
        ['FLAG_COMMISSIONS_TO_EQUITY', 'FLAG_DIVERSIFICATION', 'FLAG_GROSS_PREMIUM_TO_EQUITY',
         'FLAG_GROWTH_IN_EQUITY', 'FLAG_INVESTMENT_YIELD', 'FLAG_NET_PREMIUM_GROWTH',
         'FLAG_PREMIUM_CEDED_TO_EQUITY', 'FLAG_RESERVE_DEFICIENCY_EQUITY',
         'FLAG_TWO_YEAR_OPERATING', 'FLAG_TWO_YR_CHANGE_RESERVE_EQUITY',
         'FLAG_LIQUIDITY_RATIO']].sum(axis=1)

    def FLAG_TOTAL_ADJUSTED_FOR_ASSETS_(df):
        dff = df[['COMPANY', 'YEARS']]
        dff['%Motor_vehicle'] = (df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL'].fillna(0) + \
                                 df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL_PSV'].fillna(0) + \
                                 df['NET_EARNED_PREMIUM_MOTOR_PRIVATE'].fillna(0)) \
                                / df['NET_EARNED_PREMIUM_INCOME'].fillna(0)
        dff['%Motor_psv'] = df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL_PSV'] \
                            / df['NET_EARNED_PREMIUM_INCOME']
        dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] = np.where(dff['%Motor_psv'] >= 0.65,
                                                         df['FLAG_TOTAL'] * 1.15,
                                                         df['FLAG_TOTAL'])  # psv
        dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] = np.where(dff['%Motor_vehicle'] >= 0.65,
                                                         dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] * 1.15,
                                                         dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'])  # motor
        dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] = np.where(df['SIZE'] == 'Large Size',
                                                         dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] * 0.8,
                                                         # TA
                                                         np.where(df['SIZE'] == 'Small Size', dff[
                                                             'FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] * 1.15,
                                                                  np.where(
                                                                      (df['SIZE'] == 'Small Size') & \
                                                                      (dff[
                                                                           'FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] < 1),
                                                                      1.7,
                                                                      np.where((df[
                                                                                    'SIZE'] == 'Medium Size') & \
                                                                               (dff[
                                                                                    'FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] < 1),
                                                                               1.4, dff[
                                                                                   'FLAG_TOTAL_ADJUSTED_FOR_ASSETS']))))
        df['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'] = np.where((df['FLAG_TOTAL'].isnull()) \
                                                        | (df['TOTAL_ASSETS'].isnull()) \
                                                        | (df['TOTAL_ASSETS'] == 0), np.nan,
                                                        dff['FLAG_TOTAL_ADJUSTED_FOR_ASSETS'])
        return df

    FLAG_TOTAL_ADJUSTED_FOR_ASSETS_(df)

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

    SMOOTHING_SCORE(df)

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

        dff['%Motor_vehicle'] = (df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL'].fillna(0) + \
                                 df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL_PSV'].fillna(0) + df[
                                     'NET_EARNED_PREMIUM_MOTOR_PRIVATE'].fillna(0)) \
                                / df['NET_EARNED_PREMIUM_INCOME'].fillna(0)

        dff['%Motor_psv'] = df['NET_EARNED_PREMIUM_MOTOR_COMMERCIAL_PSV'] / df[
            'NET_EARNED_PREMIUM_INCOME']

        dff['FLAG_5YR_SMOOTHING_ADJ'] = np.where(dff['%Motor_psv'] >= 0.65, dff['fiveyr'] * 1.15,
                                                 dff['fiveyr'])  # psv
        dff['FLAG_5YR_SMOOTHING_ADJ'] = np.where(dff['%Motor_vehicle'] >= 0.65,
                                                 dff['FLAG_5YR_SMOOTHING_ADJ'] * 1.15,
                                                 dff['FLAG_5YR_SMOOTHING_ADJ'])  # motor
        dff['FLAG_5YR_SMOOTHING_ADJ'] = np.where(df['SIZE'] == 'Large Size',
                                                 dff['FLAG_5YR_SMOOTHING_ADJ'] * 0.8,  # TA
                                                 np.where(df['SIZE'] == 'Small Size',
                                                          dff['FLAG_5YR_SMOOTHING_ADJ'] * 1.15,
                                                          dff['FLAG_5YR_SMOOTHING_ADJ']))
        dff['FLAG_5YR_SMOOTHING_ADJ'] = np.where(
            (df['SIZE'] == 'Small Size') & (dff['FLAG_5YR_SMOOTHING_ADJ'] < 1), 1.7,
            np.where((df['SIZE'] == 'Medium Size') & (dff['FLAG_5YR_SMOOTHING_ADJ'] < 1),
                     1.4, dff['FLAG_5YR_SMOOTHING_ADJ']))

        dff['FLAG_3YR_SMOOTHING_ADJ'] = np.where(dff['%Motor_psv'] >= 0.65, dff['threeyr'] * 1.15,
                                                 dff['threeyr'])  # psv
        dff['FLAG_3YR_SMOOTHING_ADJ'] = np.where(dff['%Motor_vehicle'] >= 0.65,
                                                 dff['FLAG_3YR_SMOOTHING_ADJ'] * 1.15,
                                                 dff['FLAG_3YR_SMOOTHING_ADJ'])  # motor
        dff['FLAG_3YR_SMOOTHING_ADJ'] = np.where(df['SIZE'] == 'Large Size',
                                                 dff['FLAG_3YR_SMOOTHING_ADJ'] * 0.8,  # TA
                                                 np.where(df['SIZE'] == 'Small Size',
                                                          dff['FLAG_3YR_SMOOTHING_ADJ'] * 1.15,
                                                          dff['FLAG_3YR_SMOOTHING_ADJ']))
        dff['FLAG_3YR_SMOOTHING_ADJ'] = np.where(
            (df['SIZE'] == 'Small Size') & (dff['FLAG_3YR_SMOOTHING_ADJ'] < 1), 1.7,
            np.where((df['SIZE'] == 'Medium Size') & (dff['FLAG_3YR_SMOOTHING_ADJ'] < 1),
                     1.4, dff['FLAG_3YR_SMOOTHING_ADJ']))

        df['FLAG_3YR_SMOOTHING_ADJ'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), np.nan,
            dff['FLAG_3YR_SMOOTHING_ADJ'])

        df['FLAG_5YR_SMOOTHING_ADJ'] = np.where(
            (df['TOTAL_ASSETS'].isnull()) | (df['TOTAL_ASSETS'] == 0), np.nan,
            dff['FLAG_5YR_SMOOTHING_ADJ'])
        return df

    df = SMOOTHING_SCORE_ADJ(df)

    df['INSURANCE_SCORE'] = df['FLAG_3YR_SMOOTHING_ADJ']

    df['RATING'] = np.where((df['INSURANCE_SCORE'].isnull()) | (df['TOTAL_ASSETS'].isnull()), np.nan,
                            np.where(df['INSURANCE_SCORE'] >= 4.5, "D",
                                     np.where(df['INSURANCE_SCORE'] >= 2.5, "C",
                                              np.where(df['INSURANCE_SCORE'] >= 1.5, "B",
                                                       np.where(df['INSURANCE_SCORE'] >= 0, "A",
                                                                "Missing")))))

    ###saves the resulting general insurance company data

    # result_path = os.path.join(APP_FILES_PATH, "Resulting_Files")


    # # Create the result folder if it doesn't exist
    # if not os.path.exists(result_path):
    #     os.makedirs(result_path)


    # file_name = str(year) + "_general.csv"

    # file_path_general = os.path.join(result_path, file_name)

    df = df.loc[:, ~df.columns.str.contains('Unnamed')]
    df = df.apply(lambda x: round(x, 8) if x.dtype == 'float' else x)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    # print(df["YEARS"].dtype)

    # try:
    #     # Begin a transaction
    #     with db_session.begin():
    #         # Your DataFrame to SQL operation
    #         df.to_sql(locals()['general_ratio'], con=db_session.bind, if_exists='replace', index=False)

    #         # Other operations can go here
    # except Exception as e:
    #     db_session.rollback()  # Roll back on exception
    #     raise
    # finally:
    #     db_session.close()  # Ensure the session is closed when done


    df.to_sql(locals()['general_ratio'], con=engine, if_exists='replace', index=False)


    # df_final.to_csv(file_path_general)
    ratio= df

    return ratio