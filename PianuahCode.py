# import required modules
from itertools import count
from operator import concat
from pathlib import Path 
import os
import pandas as pd
import csv
import ntpath
import pyodbc
from io import StringIO
import numpy as np

params = {"input_excel" : "O:\Ricardo\messimaPIANUAH_reshima_dr_shapira.xlsx" , "output_csv_file_path" : "C:\\Users\\Ricardomc\\PythonProject\\output_csv_PIANUAH.csv"}

# reading the input excel file that contains the CaseNums, and storing the CaseNums into a python DataFrame
def read_input():
    df_from_input_excel = pd.read_excel(params["input_excel"], sheet_name="Sheet1")
    df_casenum = pd.DataFrame(columns=['CaseNum'])
    df_casenum['CaseNum'] = pd.DataFrame(df_from_input_excel['מספר מקרה'])
    #print(df_casenum)
    return df_casenum

def create_patnum_and_casenum_df():
    #CONN - PRD CONNECTION - Connecting to MSSQL server and data base
    conn = pyodbc.connect('Driver={SQL Server};'
                        'Server=BIDWHPRD;'
                        'Database=DWH_PRD;'
                        'Trusted_Connection=yes;')
    cursor = conn.cursor()
    #cursor.execute("SELECT TOP 100 * FROM DWH_PRD..PRD_DIM_CASES")

    # reading ALL CaseNums and PatNums from sql table "PRD_DIM_CASES", and storing the CaseNums and PatNums into a python DataFrame
    sql_data = pd.read_sql("SELECT CaseNum, PatNum FROM DWH_PRD..PRD_DIM_CASES", conn) 
    #print("Imported all sql data!!")

    # removing the rows where the CaseNum is not numeric(some casenums starts with "T" for some reason and are not relevant - at least for now)
    sql_data['CaseNum'] = pd.to_numeric(sql_data['CaseNum'], errors='coerce')
    sql_data = sql_data.dropna(subset=['CaseNum'])

    #joining casenum df with casenum+patnum df from sql
    mergedf = pd.merge(read_input(), sql_data, how='left', on="CaseNum")
    #for index, row in mergedf.iterrows():
        #print(index, row["PatNum"], row["CaseNum"])
    
    return mergedf

def create_output_df():
    # Creating an empty Dataframe with column names only
    df_output = pd.DataFrame(columns=['CaseNum', 'PatNum', 'Path_to_pianuah_txt_file'])
    #print(df_output)

    # for each CaseNum in our df, iterarate over files in desired directory(that matches the correct PatNum and CaseNum) and append to a list. Then add new row to the dataframe
    for index, row in create_patnum_and_casenum_df().iterrows():
        patnum = row["PatNum"]
        casenum = row["CaseNum"]
        path_list = []
        for root, dirs, files in os.walk(fr'\\focus-fs\\NamerExportText\\{patnum}\\ImagingCT\Deciphering'): 
            for filename in files:
                file_path = os.path.join(root, filename)
                if f"\\ImagingCT\\Deciphering\\00{casenum}_" in file_path:
                    path_list.append(file_path)
            df_output.loc[len(df_output.index)] = [casenum, patnum, path_list]
            #d1 = {'CaseNum': casenum, 'PatNum': patnum, 'Path_to_pianuah_txt_file': path_list}
            #temp_df_to_concat = pd.DataFrame(d1)
            #data = [df_output, temp_df_to_concat]
            #df_output = pd.concat(data)
            ##df_output = df_output.append({'CaseNum': casenum, 'PatNum': patnum, 'Path_to_pianuah_txt_file': path_list}, ignore_index=True)
    #print(df_output)
    return df_output

def final_csv():
    # Add dataframe data to output csv
    create_output_df().to_csv(params["output_csv_file_path"], index=False)
    output_csv_data = pd.read_csv(params["output_csv_file_path"])
    print(output_csv_data.head())
    return output_csv_data

def main():
    read_input()
    create_patnum_and_casenum_df()
    create_output_df()
    final_csv()

if __name__ == "__main__":
    main()
