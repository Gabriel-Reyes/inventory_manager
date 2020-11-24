import pandas as pd
import numpy as np

import gspread_dataframe as gd
import gspread as gs

import setup

from simple_salesforce import Salesforce
sf = Salesforce(username=setup.sf_username, password=setup.sf_password, security_token=setup.sf_token)


# csv export of historical sales

sales_master = pd.read_csv(setup.csv_path)


# dropping na values, filtering out samples

sales_master = sales_master.dropna()

sales_master = sales_master[sales_master['Sample'] == 'N']


# adding in datetime fields for segmentation

sales_master['Delivery Date'] = pd.to_datetime(sales_master['Delivery Date'])

sales_master['Month'] = sales_master['Delivery Date'].dt.month
sales_master['Year'] = sales_master['Delivery Date'].dt.year
sales_master['Week'] = sales_master['Delivery Date'].dt.isocalendar().week



# limiting data to only directly purchased and managed inventory

sales_master_no_dsw = sales_master[sales_master['Warehouse'] != 'DSW']


# global monthly sales

ind = ['Item Description: Product Family', 'Item Description: Size']
cols = ['Year', 'Month']

monthly_sales_global = pd.pivot_table(sales_master_no_dsw, values='Cases Sold', index=ind, columns=cols, aggfunc=np.sum).reset_index()
monthly_sales_global = monthly_sales_global.fillna(0)


# monthly sales by warehouse

warehouses = ['SBC1', 'CAW1', 'ILL1', 'VAW1']

ind = ['Item Description: Product Family', 'Item Description: Size', 'Warehouse']
cols = ['Year', 'Month']

monthly_sales_wh = pd.pivot_table(sales_master_no_dsw, values='Cases Sold', index=ind, columns=cols, aggfunc=np.sum).reset_index()

monthly_sales_sbc1 = monthly_sales_wh[monthly_sales_wh['Warehouse'] == warehouses[0]].fillna(0)
monthly_sales_caw1 = monthly_sales_wh[monthly_sales_wh['Warehouse'] == warehouses[1]].fillna(0)
monthly_sales_ill1 = monthly_sales_wh[monthly_sales_wh['Warehouse'] == warehouses[2]].fillna(0)
monthly_sales_vaw1 = monthly_sales_wh[monthly_sales_wh['Warehouse'] == warehouses[3]].fillna(0)


# main inventory template, data from Salesforce product object 

base_template = sf.query("""SELECT ProductCode, Product_Family__c, Description, Current_Vintage__c,
                            Country__c, Size__c, Bt_Cs__c,
                            Item_Cost_SBC__c, Item_Cost_CA__c, Item_Cost_ILL__c, Item_Cost_VA__c,
                            Cases_On_Hand__c, Total_Committed_Cases__c, Total_Inv_Value__c,
                            SBC_Cases_OH__c, NY_NJ_Committed_Cases__c, NY_NJ_Available_Cases__c, SBC_Inv_Value__c,
                            CA_Cases_OH__c, CA_Committed_Cases__c, CA_Available_Cases__c, CA_Inv_Value__c,
                            ILL_Cases_OH__c, ILL_Committed_Cases__c, ILL_Available_Cases__c, ILL_Inv_Value__c,
                            VA_Cases_OH__c, VA_Committed_Cases__c, VA_Available_Cases__c, VA_Inv_Value__c,
                            SBC_Cases_on_Order__c, CAW1_Cases_on_Order__c, Cases__c, Next_Drop_Date__c
                            FROM Product2
                            WHERE IsActive = TRUE
                            ORDER BY Description""")


# cases sold: today - x days global; Salesforce queries from invoice_lines object

sf_this_month = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, SUM(Cases_Sold__c) cases_sold_this_month
                            FROM Invoice_line__c
                            WHERE Cases_Sold__c > 0
                            AND Delivery_Date_from_Invoice__c = THIS_MONTH
                            GROUP BY Item_No__r.ProductCode, Item_Name__c
                            ORDER BY Item_Name__c""")

sf_t7 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, SUM(Cases_Sold__c) cases_sold_t7
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:7
                    GROUP BY Item_No__r.ProductCode, Item_Name__c
                    ORDER BY Item_Name__c""")

sf_t30 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, SUM(Cases_Sold__c) cases_sold_t30
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:30
                    GROUP BY Item_No__r.ProductCode, Item_Name__c
                    ORDER BY Item_Name__c""")

sf_t60 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, SUM(Cases_Sold__c) cases_sold_t60
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:60
                    GROUP BY Item_No__r.ProductCode, Item_Name__c
                    ORDER BY Item_Name__c""")

sf_t90 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, SUM(Cases_Sold__c) cases_sold_t90
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:90
                    GROUP BY Item_No__r.ProductCode, Item_Name__c
                    ORDER BY Item_Name__c""")

sf_t120 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, SUM(Cases_Sold__c) cases_sold_t120
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:120
                    GROUP BY Item_No__r.ProductCode, Item_Name__c
                    ORDER BY Item_Name__c""")


# cases sold: today - x days by warehouse; Salesforce queries from invoice_lines object

sf_wh_this_month = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, Warehouse__c,
                            SUM(Cases_Sold__c) cases_sold_this_month
                            FROM Invoice_line__c
                            WHERE Cases_Sold__c > 0
                            AND Delivery_Date_from_Invoice__c = THIS_MONTH
                            GROUP BY Item_No__r.ProductCode, Item_Name__c, Warehouse__c
                            ORDER BY Item_Name__c""")

sf_wh_t7 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, Warehouse__c,
                    SUM(Cases_Sold__c) cases_sold_t7
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:7
                    GROUP BY Item_No__r.ProductCode, Item_Name__c, Warehouse__c
                    ORDER BY Item_Name__c""")


sf_wh_t30 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, Warehouse__c,
                    SUM(Cases_Sold__c) cases_sold_t30
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:30
                    GROUP BY Item_No__r.ProductCode, Item_Name__c, Warehouse__c
                    ORDER BY Item_Name__c""")


sf_wh_t60 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, Warehouse__c,
                    SUM(Cases_Sold__c) cases_sold_t60
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:60
                    GROUP BY Item_No__r.ProductCode, Item_Name__c, Warehouse__c
                    ORDER BY Item_Name__c""")


sf_wh_t90 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, Warehouse__c,
                    SUM(Cases_Sold__c) cases_sold_t90
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:90
                    GROUP BY Item_No__r.ProductCode, Item_Name__c, Warehouse__c
                    ORDER BY Item_Name__c""")


sf_wh_t120 = sf.query("""SELECT Item_No__r.ProductCode, Item_Name__c, Warehouse__c,
                    SUM(Cases_Sold__c) cases_sold_t120
                    FROM Invoice_line__c
                    WHERE Cases_Sold__c > 0
                    AND Delivery_Date_from_Invoice__c = Last_N_Days:120
                    GROUP BY Item_No__r.ProductCode, Item_Name__c, Warehouse__c
                    ORDER BY Item_Name__c""")


# cs sold t-x global, formatting SOQL queries into dataframes

this_month = pd.DataFrame(sf_this_month['records']).iloc[:, 1:]
t_7 = pd.DataFrame(sf_t7['records']).iloc[:, 1:]
t_30 = pd.DataFrame(sf_t30['records']).iloc[:, 1:]
t_60 = pd.DataFrame(sf_t60['records']).iloc[:, 1:]
t_90 = pd.DataFrame(sf_t90['records']).iloc[:, 1:]
t_120 = pd.DataFrame(sf_t120['records']).iloc[:, 1:]


# cs sold t-x by warehouse, formatting SOQL queries into dataframes

t_wh_this_month = pd.DataFrame(sf_wh_this_month['records']).iloc[:, 1:]
t_wh7 = pd.DataFrame(sf_wh_t7['records']).iloc[:, 1:]
t_wh30 = pd.DataFrame(sf_wh_t30['records']).iloc[:, 1:]
t_wh60 = pd.DataFrame(sf_wh_t60['records']).iloc[:, 1:]
t_wh90 = pd.DataFrame(sf_wh_t90['records']).iloc[:, 1:]
t_wh120 = pd.DataFrame(sf_wh_t120['records']).iloc[:, 1:]


# joining all t-x global queries into single df

tx_dfs = [t_120, t_90, t_60, t_30, t_7, this_month]
tx_dfs = [df.set_index('ProductCode') for df in tx_dfs]
tx_joins = [df.drop(columns='Item_Name__c') for df in tx_dfs[1:]]
tx_global = tx_dfs[0].join(tx_joins).fillna(0)


# joining all t-x warehouse queries into single df

tx_whdfs = [t_wh120, t_wh90, t_wh60, t_wh30, t_wh7, t_wh_this_month]
tx_whdfs = [df.set_index(['ProductCode', 'Warehouse__c']) for df in tx_whdfs]
tx_whjoins = [df.drop(columns='Item_Name__c') for df in tx_whdfs[1:]]
tx_wh_all = (tx_whdfs[0].
            join(tx_whjoins[0]).join(tx_whjoins[1]).join(tx_whjoins[2]).join(tx_whjoins[3]).join(tx_whjoins[4])
            .fillna(0))

# create list of t-x dataframes for each warehouse, callable based off position in warehouse list

tx_whs = [tx_wh_all[tx_wh_all.index.get_level_values(1) == wh] for wh in warehouses]


# creating base template that time-interval sales data can be joined onto by product code

base_table = pd.DataFrame(base_template['records']).iloc[:, 1:]
base_table = base_table.set_index('ProductCode')
base_table['Next_Drop_Date__c'] = pd.to_datetime(base_table['Next_Drop_Date__c'])
base_table['Size__c'] = base_table['Size__c'].astype(float)


# creation of all base templates specifc to each depletion report style

global_base = base_table[['Product_Family__c', 'Description', 'Current_Vintage__c', 'Country__c', 'Size__c',
                        'Bt_Cs__c', 'Item_Cost_SBC__c', 'Cases_On_Hand__c', 'Total_Committed_Cases__c',
                         'Total_Inv_Value__c', 'SBC_Cases_on_Order__c', 'Cases__c', 'Next_Drop_Date__c']]

sbc1_base = base_table[['Product_Family__c', 'Description', 'Current_Vintage__c', 'Country__c', 'Size__c',
                       'Bt_Cs__c', 'Item_Cost_SBC__c', 'SBC_Cases_OH__c', 'NY_NJ_Committed_Cases__c',
                       'NY_NJ_Available_Cases__c', 'SBC_Inv_Value__c', 'SBC_Cases_on_Order__c', 'Cases__c',
                       'Next_Drop_Date__c']]

caw1_base = base_table[['Product_Family__c', 'Description', 'Current_Vintage__c', 'Country__c', 'Size__c',
                       'Bt_Cs__c', 'Item_Cost_CA__c', 'CA_Cases_OH__c', 'CA_Committed_Cases__c',
                       'CA_Available_Cases__c', 'CA_Inv_Value__c', 'CAW1_Cases_on_Order__c']]

ill1_base = base_table[['Product_Family__c', 'Description', 'Current_Vintage__c', 'Country__c', 'Size__c',
                       'Bt_Cs__c', 'Item_Cost_ILL__c', 'ILL_Cases_OH__c', 'ILL_Committed_Cases__c',
                       'ILL_Available_Cases__c', 'ILL_Inv_Value__c']]

vaw1_base = base_table[['Product_Family__c', 'Description', 'Current_Vintage__c', 'Country__c', 'Size__c',
                       'Bt_Cs__c', 'Item_Cost_VA__c', 'VA_Cases_OH__c', 'VA_Committed_Cases__c',
                       'VA_Available_Cases__c', 'VA_Inv_Value__c']]


# joining t-x sales data to respective base template

global_report = (global_base.join(tx_global)
                 .drop('Item_Name__c', axis=1)
                 .sort_values('Description'))

global_report.iloc[:, -5:] = global_report.iloc[:, -5:].fillna(0)


sbc1 = (sbc1_base.join(tx_whs[0].reset_index(level=1))
        .drop(['Warehouse__c', 'Item_Name__c'], axis=1)
        .sort_values('Description'))

sbc1 = sbc1[(sbc1['SBC_Cases_OH__c'] > 0) | (sbc1['SBC_Cases_on_Order__c'] > 0)]

sbc1.iloc[:, -5:] = sbc1.iloc[:, -5:].fillna(0)


caw1 = (caw1_base.join(tx_whs[1].reset_index(level=1))
        .drop(['Warehouse__c', 'Item_Name__c'], axis=1)
        .sort_values('Description'))

caw1 = caw1[(caw1['CA_Cases_OH__c'] > 0) | (caw1['CAW1_Cases_on_Order__c'] > 0)]

caw1.iloc[:, -5:] = caw1.iloc[:, -5:].fillna(0)


ill1 = (ill1_base.join(tx_whs[2].reset_index(level=1))
        .drop(['Warehouse__c', 'Item_Name__c'], axis=1)
        .sort_values('Description'))

ill1 = ill1[ill1['ILL_Cases_OH__c'] > 0]

ill1.iloc[:, -5:] = ill1.iloc[:, -5:].fillna(0)


vaw1 = (vaw1_base.join(tx_whs[3].reset_index(level=1))
        .drop(['Warehouse__c', 'Item_Name__c'], axis=1)
        .sort_values('Description'))

vaw1 = vaw1[vaw1['VA_Cases_OH__c'] > 0]

vaw1.iloc[:, -5:] = vaw1.iloc[:, -5:].fillna(0)

inv_reports = [global_report, sbc1, caw1, ill1, vaw1]

global_report['OH_months_inventory'] = ((global_report['Cases_On_Hand__c']
                                      - global_report['Total_Committed_Cases__c'])
                                     / global_report['cases_sold_t30'])

sbc1['OH_months_inventory'] = (sbc1['NY_NJ_Available_Cases__c'] / sbc1['cases_sold_t30']).round(1)
caw1['OH_months_inventory'] = (caw1['CA_Available_Cases__c'] / caw1['cases_sold_t30']).round(1)
ill1['OH_months_inventory'] = (ill1['ILL_Available_Cases__c'] / ill1['cases_sold_t30']).round(1)
vaw1['OH_months_inventory'] = (vaw1['VA_Available_Cases__c'] / vaw1['cases_sold_t30']).round(1)

for df in inv_reports:
    df['OH_months_inventory'] = df['OH_months_inventory'].replace([np.inf, -np.inf], np.nan).round(1)
    df.reset_index(inplace=True)


# joining all historical monthly sales data to reports


## global master

global_joined = global_report.merge(monthly_sales_global, how='left', left_on=['Product_Family__c', 'Size__c'],
                            right_on=['Item Description: Product Family', 'Item Description: Size'])

global_master = global_joined.drop([('Item Description: Product Family', ''), ('Item Description: Size', '')], axis=1)


## sbc1 master

sbc1_joined = sbc1.merge(monthly_sales_sbc1, how='left', left_on=['Product_Family__c', 'Size__c'],
                         right_on=['Item Description: Product Family', 'Item Description: Size'])

sbc1_master = sbc1_joined.drop([('Item Description: Product Family', ''), ('Item Description: Size', ''),
                               ('Warehouse', '')], axis=1)


## caw1 master

caw1_joined = caw1.merge(monthly_sales_caw1, how='left', left_on=['Product_Family__c', 'Size__c'],
                         right_on=['Item Description: Product Family', 'Item Description: Size'])

caw1_master = caw1_joined.drop([('Item Description: Product Family', ''), ('Item Description: Size', ''),
                               ('Warehouse', '')], axis=1)


## ill1 master

ill1_joined = ill1.merge(monthly_sales_ill1, how='left', left_on=['Product_Family__c', 'Size__c'],
                         right_on=['Item Description: Product Family', 'Item Description: Size'])

ill1_master = ill1_joined.drop([('Item Description: Product Family', ''), ('Item Description: Size', ''),
                               ('Warehouse', '')], axis=1)


## vaw1 master

vaw1_joined = vaw1.merge(monthly_sales_vaw1, how='left', left_on=['Product_Family__c', 'Size__c'],
                         right_on=['Item Description: Product Family', 'Item Description: Size'])

vaw1_master = vaw1_joined.drop([('Item Description: Product Family', ''), ('Item Description: Size', ''),
                               ('Warehouse', '')], axis=1)


# list of master inventory reports to perform final modifications on

master_dfs = [global_master, sbc1_master, caw1_master, ill1_master, vaw1_master]


# function list to modify final reports

## function to subtract X amount of months from current date, returns tuple of (year, month)

def month_sbtrkt(months_from_today):
    year = (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=months_from_today)).year
    month = (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=months_from_today)).month
    return (year, month)

## function to print month name and year 

def month_namer(months_from_today):
    year = (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=months_from_today)).year
    month = (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=months_from_today)).month_name()
    return month, year


## function to predict next X months of sales

def depletion_estimator_6mons(df):
    last_3_mons_YoY = df['last_3_months_YoY']
    t30 = df['cases_sold_t30']
    estimates = {}
    for i in range(1,7):            
        x = np.where(last_3_mons_YoY.isnull() == False,
            ((last_3_mons_YoY * df[month_sbtrkt(-12 + i)]) + df[month_sbtrkt(-12 + i)]).round(2),
            t30)            
        estimates['forecast:', month_namer(i)] = x
            
    return pd.DataFrame(estimates)

## function to provide predicted starting inventory level for next X months, warehouses that intake product

def cases_oh_estimator_mains(df, csohkey, dropdatekey, dropqtykey):
    setup = pd.DataFrame()
    oh = pd.DataFrame()
    cases_left_this_month = df['projected_sales_current_month'] - df['cases_sold_this_month']
    setup['cases_left_this_month'] = cases_left_this_month
    for i in range (1,7):
        y = np.where(df[dropdatekey].dt.month == (pd.Timestamp.today() + pd.tseries.offsets.DateOffset(months=(i-1))).month,
        df[dropqtykey] - df['forecast:', month_namer(i)], 0 - df['forecast:', month_namer(i)])
        setup['delta:', month_namer(i)] = y

    oh['Estimated Cases OH', month_namer(1)] = np.where(df[dropdatekey].dt.month == pd.Timestamp.today().month,
    df[csohkey] + df[dropqtykey] - setup['cases_left_this_month'],
    df[csohkey] - setup['cases_left_this_month'])

    for i in range(2,7):
        oh['Estimated Cases OH', month_namer(i)] = oh['Estimated Cases OH', month_namer(i-1)] + setup['delta:', month_namer(i-1)]

    return oh

## function to provide predicted starting inventory for next X months, warehouses that have inventory transferred

def cases_oh_estimator_secondary(df, csohkey):
    setup = pd.DataFrame()
    oh = pd.DataFrame()
    cases_left_this_month = df['projected_sales_current_month'] - df['cases_sold_this_month']
    setup['cases_left_this_month'] = cases_left_this_month

    oh['Estimated Cases OH', month_namer(1)] = df[csohkey] - setup['cases_left_this_month']

    for i in range(2,7):
        oh['Estimated Cases OH', month_namer(i)] = oh['Estimated Cases OH', month_namer(i-1)] - df['forecast:', month_namer(i-1)]

    return oh


for df in master_dfs:
    
    # segment out cases sold into 30 day intervals
    df['cases_sold_t120-t90'] = df['cases_sold_t120'] - df['cases_sold_t90']
    df['cases_sold_t90-t60'] = df['cases_sold_t90'] - df['cases_sold_t60']
    df['cases_sold_t60-t30'] = df['cases_sold_t60'] - df['cases_sold_t30']
    
    # add 30 day trend
    df['30_day_trend'] = (df['cases_sold_t30'] - df['cases_sold_t60-t30']) / df['cases_sold_t60-t30']
    df['30_day_trend'] = df['30_day_trend'].replace([np.inf, -np.inf], np.nan).round(1)

    # add 7 day trend
    df['7_day_trend'] = (df['cases_sold_t7'] - (df['cases_sold_t30']*(7/30))) / (df['cases_sold_t30']*(7/30))
    df['7_day_trend'] = df['7_day_trend'].replace([np.inf, -np.inf], np.nan).round(1)

    # add last 3 month YoY trend
    df['last_3_months_YoY'] = (((df[month_sbtrkt(-1)] +
                                 df[month_sbtrkt(-2)] +
                                 df[month_sbtrkt(-3)]) -
                                    (df[month_sbtrkt(-13)] +
                                     df[month_sbtrkt(-14)] +
                                     df[month_sbtrkt(-15)])) /
                                        (df[month_sbtrkt(-13)] +
                                         df[month_sbtrkt(-14)] +
                                         df[month_sbtrkt(-15)])).replace([np.inf, -np.inf], np.nan).round(2)

    # add estimator for current month total sales
    df['projected_sales_current_month'] = (df['cases_sold_this_month'] /
                                            (pd.Timestamp.today().day / pd.Timestamp.today().days_in_month))



# add simple future depletion estimates
global_pred = global_master.join(depletion_estimator_6mons(global_master))
sbc1_pred = sbc1_master.join(depletion_estimator_6mons(sbc1_master))
caw1_pred = caw1_master.join(depletion_estimator_6mons(caw1_master))
ill1_pred = ill1_master.join(depletion_estimator_6mons(ill1_master))
vaw1_pred = vaw1_master.join(depletion_estimator_6mons(vaw1_master))


# add future cases on hand estimators for upcoming months
global_final = global_pred.join(cases_oh_estimator_mains(global_pred, 'Cases_On_Hand__c', 'Next_Drop_Date__c', 'Cases__c'))
sbc1_final = sbc1_pred.join(cases_oh_estimator_mains(sbc1_pred, 'SBC_Cases_OH__c', 'Next_Drop_Date__c', 'Cases__c'))
caw1_final = caw1_pred.join(cases_oh_estimator_secondary(caw1_pred, 'CA_Cases_OH__c'))
ill1_final = ill1_pred.join(cases_oh_estimator_secondary(ill1_pred, 'ILL_Cases_OH__c'))
vaw1_final = vaw1_pred.join(cases_oh_estimator_secondary(vaw1_pred, 'VA_Cases_OH__c'))

gc = gs.service_account(filename='service_acct.json')

spreadsheet = gc.open_by_key('1qy2QqWN6C9df5K7hEh8hwDfG-CP2bnveoXyuLdzKeKY')

sheet_global = spreadsheet.worksheet('Global')
sheet_nj = spreadsheet.worksheet('NJ')
sheet_ca = spreadsheet.worksheet('CA')
sheet_ill = spreadsheet.worksheet('ILL')
sheet_va = spreadsheet.worksheet('VA')

sheets = [sheet_global, sheet_nj, sheet_ca, sheet_ill, sheet_va]

def reset_sheet(name):
    if name == 'Global':
        sheet_global.clear()
        gd.set_with_dataframe(sheet_global, global_final)
    if name == 'NJ':
        sheet_nj.clear()
        gd.set_with_dataframe(sheet_nj, sbc1_final)
    if name == 'CA':
        sheet_ca.clear()
        gd.set_with_dataframe(sheet_ca, caw1_final)        
    if name == 'ILL':
        sheet_ill.clear()
        gd.set_with_dataframe(sheet_ill, ill1_final)
    if name == 'VA':
        sheet_va.clear()
        gd.set_with_dataframe(sheet_va, vaw1_final)
        
def spreadsheet_reset():
    sheet_global.clear()
    gd.set_with_dataframe(sheet_global, global_final)
    sheet_nj.clear()
    gd.set_with_dataframe(sheet_nj, sbc1_final)
    sheet_ca.clear()
    gd.set_with_dataframe(sheet_ca, caw1_final)
    sheet_ill.clear()
    gd.set_with_dataframe(sheet_ill, ill1_final)
    sheet_va.clear()
    gd.set_with_dataframe(sheet_va, vaw1_final)