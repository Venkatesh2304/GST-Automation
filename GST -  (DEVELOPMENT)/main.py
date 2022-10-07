import pandas as pd
import numpy as np
import copy
import re
import time 
from functools import *
from collections import defaultdict

import invoicedb
import sessiondb
import gstsite 
import jsonmake

period = "062022"
class creds() : 
      pass 
cred = creds()
cred.gstuser = "DEVAKI9999"
cred.gstpwd = "Mosl2004@"
      
def read_excel() :
    df= pd.read_csv("C:\\Users\\Venkatesh\\Desktop\\GST\\DEVAKI\\05-2022\\gst.csv")
    df = df.astype({"HSN" : str})
    reverse_columns = ["Taxable","Amount - Central Tax" ,"Invoice Value","Amount - State/UT Tax"]
    df["Tax - Central Tax"] = df["Tax - Central Tax"]  * 2 
    df[reverse_columns] = df[reverse_columns].where(df.Transactions != "SALES RETURN" , -df[reverse_columns])
    df['HSN'] = df['HSN'].apply(lambda hsn : hsn.replace('.',''))
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
    columns = { "Invoice No":"inum", "Invoice Date" : "idt","Invoice Value":"val","Amount - Central Tax":"camt","Amount - State/UT Tax":"samt",
                 "Tax - Central Tax" : "rt","GSTIN of Recipient" : "ctin","Outlet Name":"buyer","Outlet Code":"buyerid","Taxable":"txval","HSN":"hsn_sc","HSN Description":"desc",
                 "UOM" : "uqc","Total Quantity":"qty","docs" : "docs" ,"type" : "type"}
    df = df.rename(columns = columns)
    df = df[[ key for key in columns.values() ]]
    df["einvoice"] = False 
    df["override"] = False 
    return df.to_dict('records')

def main() :
    data = read_excel()
    invoicedb.reset(cred,period)
    invoicedb.addinvs(cred,period,data)
    jsonmake.main(cred,period)

jsonmake.report(cred,period)