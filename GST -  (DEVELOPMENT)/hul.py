import pandas as pd
import numpy as np
import copy
import re
import time 
from functools import *
from collections import defaultdict
from io import StringIO 

import invoicedb
import sessiondb
import gstsite 
import jsonmake

import ikea 
def try_split(code) : 
    try : 
        return code.split("-")[1]
    except : 
        return code 
def clean(df) : 
      df["Outlet Code"] = df["Outlet Code"].fillna("D-HUL")
      df["code"] = df["Outlet Code"].apply(lambda code : try_split(code) )
      code = df[df["GSTIN of Recipient"].notna()][["code","GSTIN of Recipient"]]
      code = code.drop_duplicates(keep="last")
      code = code.to_dict("split")["data"] 
      gst = { data[0] : data[1] for data in code }
      df["Old Gst"] = df["GSTIN of Recipient"].copy()
      df["GSTIN of Recipient"] = df["code"].apply(lambda code : gst[code] if code in gst.keys()  else  np.nan )
      altered  = df[df["GSTIN of Recipient"].notna()][df["Old Gst"] != df["GSTIN of Recipient"]][df["Old Gst"].isna()]
      altered = altered.rename(columns = {"Old Gst" : "remarks"} )
      temp = altered.groupby(by =["Invoice No"],as_index=False).sum()
      clean_data =  []      
      for  inum , values in temp.iterrows() : 
            inum = values["Invoice No"]
            row =  altered[altered["Invoice No"] == inum].reset_index(drop=True).iloc[0]
            for key , value in dict(values).items() : 
                row[key] = value 
            clean_data.append(row)
      clean_data = pd.DataFrame(clean_data) 
      clean_data["Outlet Name"] = clean_data["Invoice No"].apply( lambda inum : altered[altered["Invoice No"] == inum].iloc[0]["Outlet Name"] )
      return clean_data 
          
def getinvoicedata(cred,period,add_process) : 
    cred_datas = cred.data 
    if type(cred_datas) == dict : 
        cred_datas = [cred_datas] 
    inv_data = []
    for cred_data in cred_datas : 
         inv_data += getinvoicedata_each(cred,cred_data,period,add_process)
    return inv_data

def getinvoicedata_each(cred,cred_data,period,add_process) :

    data = ikea.excel_download(cred_data,period)
    #print(data)
    df= pd.read_csv(StringIO(data))
    #df.to_excel("gst.xlsx")
    #df= pd.read_csv("C:\\Users\\Venkatesh\\Desktop\\GST\\DEVAKI\\05-2022\\gst.csv")    
    df = df.astype({"HSN" : str})
    df["Invoice Date"] = df["Invoice Date"].apply(lambda d : d.replace("/","-") )
    reverse_columns = ["Taxable","Amount - Central Tax" ,"Invoice Value","Amount - State/UT Tax"]
    df["Tax - Central Tax"] = df["Tax - Central Tax"]  * 2 
    df["Invoice Value"] = df["Invoice Value"].round(2)
    df[reverse_columns] = df[reverse_columns].where(df.Transactions != "SALES RETURN" , -df[reverse_columns])
    df["Invoice No"] = df["Invoice No"].where(df.Transactions != "SALES RETURN" , df["Debit/Credit No"])
    
    df['HSN'] = df['HSN'].apply(lambda hsn : hsn.replace('.',''))
    columns = { "Invoice No":"inum", "Invoice Date" : "idt","Invoice Value":"val","Amount - Central Tax":"camt","Amount - State/UT Tax":"samt",
                 "Tax - Central Tax" : "rt","GSTIN of Recipient" : "ctin","Outlet Name":"buyer","Outlet Code":"buyerid","Taxable":"txval","HSN":"hsn_sc","HSN Description":"desc",
                 "UOM" : "uqc","Total Quantity":"qty","docs" : "docs" ,"type" : "type"}
                 
    if cred_data["clean"] : 
       clean_data = clean(df)
       clean_data = clean_data.rename(columns = columns)
       clean_data["from"] = "B2C"
       clean_data["to"] = "B2B"
       clean_data["action"] = "CHANGE TYPE"
       clean_data["remarks"] = "This store has gstnumber in other division . So , We added gst number ."
       add_process(cred,period,clean_data)

    def gettype(row) : 
        if row["GSTIN of Recipient"] != row["GSTIN of Recipient"]  :
            return "b2cs"
        elif row["Transactions"] != "SALES RETURN" : 
            return "b2b"
        else : 
            return "cdnr"
    df["type"] = df.apply( lambda row : gettype(row) , axis = 1 )
    nan_to_empty = lambda  x :  "" if x!=x or x == None else  x 
    df["docs"] = df.apply( 
        lambda row : ",".join([nan_to_empty(row[col]) for col in ["Debit/Credit No","Original Invoice No"]]) ,
        axis = 1 )  
    df = df.rename(columns = columns)
       

    df = df[[ key for key in columns.values() ]]
    df["einvoice"] = False 
    df["override"] = False 
    return df.to_dict('records')