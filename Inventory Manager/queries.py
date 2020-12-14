import pandas as pd
import setup
from classes import cash_flow, payables, receivables

from simple_salesforce import Salesforce

sf = Salesforce(username=setup.sf_username, password=setup.sf_password, security_token=setup.sf_token)


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


# accounts payable and accounts receivable queries 

sf_ap = sf.query('''SELECT Account__c, Amount_Paid__c, Doc_Currency__c, Doc_Rate__c, DocType__c, Aging_Days__c,
                    Total__c, Net_Balance__c, EURO_Balance__c, Due_Date__c, Order_Status__c
                    FROM AP_Invoice__c
                    WHERE Order_Status__c = 'O'
                    AND DocType__c = 'IN' ''')

sf_ar_cq = sf.query('''SELECT Account__c, DBA__c, Balance__c, Due_Date__c, Aging__c, Invoice_Status__c, Document_Type__c
                    FROM Invoice__c
                    WHERE Invoice_Status__c = 'O'
                    AND (NOT DBA__c LIKE '%Sample%')
                    AND (External_ID__c LIKE 'FQZ%')
                    AND Due_Date__c = THIS_QUARTER ''')

sf_ar_before_cq = sf.query('''SELECT Account__c, DBA__c, Balance__c, Due_Date__c, Aging__c, Invoice_Status__c, Document_Type__c
                    FROM Invoice__c
                    WHERE Invoice_Status__c = 'O'
                    AND (NOT DBA__c LIKE '%Sample%')
                    AND (External_ID__c LIKE 'FQZ%')
                    AND Due_Date__c < THIS_QUARTER ''')

sf_ar_after_cq = sf.query('''SELECT Account__c, DBA__c, Balance__c, Due_Date__c, Aging__c, Invoice_Status__c, Document_Type__c
                    FROM Invoice__c
                    WHERE Invoice_Status__c = 'O'
                    AND (NOT DBA__c LIKE '%Sample%')
                    AND (External_ID__c LIKE 'FQZ%')
                    AND Due_Date__c > THIS_QUARTER ''')


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

# columns to rename now to have polished end dataframes

rename_cols_tx = {'cases_sold_t30':'Cases Sold: T-30',
                'cases_sold_t7':'Cases Sold: T-7',
                'cases_sold_this_month':'Cases Sold This Month'}


# joining all t-x global queries into single df

tx_dfs = [t_120, t_90, t_60, t_30, t_7, this_month]
tx_dfs = [df.set_index('ProductCode') for df in tx_dfs]
tx_joins = [df.drop(columns='Item_Name__c') for df in tx_dfs[1:]]
tx_global = tx_dfs[0].join(tx_joins).fillna(0)
tx_global = tx_global.rename(columns=rename_cols_tx)


# joining all t-x warehouse queries into single df

tx_whdfs = [t_wh120, t_wh90, t_wh60, t_wh30, t_wh7, t_wh_this_month]
tx_whdfs = [df.set_index(['ProductCode', 'Warehouse__c']) for df in tx_whdfs]
tx_whjoins = [df.drop(columns='Item_Name__c') for df in tx_whdfs[1:]]
tx_wh_all = (tx_whdfs[0].
            join(tx_whjoins[0]).join(tx_whjoins[1]).join(tx_whjoins[2]).join(tx_whjoins[3]).join(tx_whjoins[4])
            .fillna(0))
tx_wh_all = tx_wh_all.rename(columns=rename_cols_tx)


# creating base template that time-interval sales data can be joined onto by product code

base_table = pd.DataFrame(base_template['records']).iloc[:, 1:]
base_table = base_table.set_index('ProductCode')
base_table['Next_Drop_Date__c'] = pd.to_datetime(base_table['Next_Drop_Date__c'])
base_table['Size__c'] = base_table['Size__c'].astype(float)

# renaming base template columns to be more understandable

rename_cols_base = {'Product_Family__c':'Product Family', 'Current_Vintage__c':'Current Vintage','Country__c':'Country',
'Size__c':'Size', 'Bt_Cs__c':'Bottles/Case', 'Item_Cost_SBC__c':'Item Cost NJ', 'Item_Cost_CA__c':'Item Cost CA',
'Item_Cost_ILL__c':'Item Cost IL', 'Item_Cost_VA__c':'Item Cost VA', 'Cases_On_Hand__c':'Total Cases OH',
'Total_Committed_Cases__c':'Total Cases Committed', 'Total_Inv_Value__c':'Total Inv Value', 'SBC_Cases_OH__c':'NJ Cases OH',
'NY_NJ_Committed_Cases__c':'NJ Cases Committed', 'NY_NJ_Available_Cases__c':'NJ Cases Available',
'SBC_Inv_Value__c':'NJ Inv Value', 'CA_Cases_OH__c':'CA Cases OH', 'CA_Committed_Cases__c':'CA Cases Committed',
'CA_Available_Cases__c':'CA Cases Available', 'CA_Inv_Value__c':'CA Inv Value', 'ILL_Cases_OH__c':'IL Cases OH',
'ILL_Committed_Cases__c':'IL Cases Comitted', 'ILL_Available_Cases__c':'IL Cases Available', 'ILL_Inv_Value__c':'IL Inv Value',
'VA_Cases_OH__c':'VA Cases OH', 'VA_Committed_Cases__c':'VA Cases Committed', 'VA_Available_Cases__c':'VA Cases Available',
'VA_Inv_Value__c':'VA Inv Value', 'SBC_Cases_on_Order__c':'NJ Cases on Order', 'CAW1_Cases_on_Order__c':'CA Cases on Order',
'Cases__c':'Cases on Next Drop', 'Next_Drop_Date__c':'Next Drop Date'}

base_table = base_table.rename(columns=rename_cols_base)


# accounts payable df

ap = pd.DataFrame(sf_ap['records']).iloc[:, 1:]

rename_cols_ap = {'Account__c':'Account', 'Amount_Paid__c':'Amount Paid', 'Doc_Currency__c':'Doc Currency',
                    'Doc_Rate__c':'Doc Rate', 'DocType__c':'Doc Type', 'Aging_Days__c':'Days Aging',
                    'Total__c':'Total', 'Net_Balance__c':'Net Balance', 'EURO_Balance__c':'Euro Balance',
                    'Due_Date__c':'Due Date', 'Order_Status__c':'Order Status'}

ap = ap.rename(columns=rename_cols_ap)


# accounts receivable df

sf_ar_cq = pd.DataFrame(sf_ar_cq['records']).iloc[:, 1:]
sf_ar_before_cq = pd.DataFrame(sf_ar_before_cq['records']).iloc[:, 1:]
sf_ar_after_cq = pd.DataFrame(sf_ar_after_cq['records']).iloc[:, 1:]

ar = pd.concat([sf_ar_cq, sf_ar_before_cq, sf_ar_after_cq])

rename_cols_ar = {'Account__c':'Account', 'DBA__c':'DBA', 'Balance__c':'Balance', 'Due_Date__c':'Due Date',
                    'Aging__c':'Days Aging', 'Invoice_Status__c':'Invoice Status', 'Document_Type__c':'Doc Type'}

ar = ar.rename(columns=rename_cols_ar)


# joining AP and AR dataframes

ap = payables(ap).monthly()
ap['Type'] = 'Accounts Payable'
ar = receivables(ar).monthly()
ar['Type'] = 'Accounts Receivable'
cash_flow = pd.concat([ap,ar])