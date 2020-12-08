# Inventory Depletion & Forecasting by Warehouse

It is a constant challenge to be able to actively monitor inventory depletions across multiple warehouses. This program aims to break down inventory and sales by each site, identify sudden growths and declines as they happen, and run historical trend analysis on each product in real time to predict future sales.

In addition, accounts receivable along with accounts payable data is pulled to provide insight into current cash flow health

This program is currently designed to extract data from Salesforce via SOQL queries, combine and transform data with a large historical csv export, and load dataframes into google sheets to allow for ease of use and understanding in a widely known format.
