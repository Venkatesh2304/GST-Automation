from numpy.lib.function_base import i0
from pandas.core.reshape.pivot import pivot_table
import gstsite 
import invoicedb 
import sessiondb
import re
import pandas as pd
from collections import defaultdict
import json 

detail = lambda df,key : df[key].iloc[0]
sums = lambda df,key : round(sum(df[key]),2)
sums_int = lambda df,key : int(sum(df[key]))
is_constant = lambda func : (type(func) == str or type(func) == float or type(func) == int)
val_keys = ["txval" ,"camt", "samt"]

#lambda functions 
getfiledstatus = lambda  cred,period : gstsite.getfiledstatus(cred,period)


def getvalue(inv) : 
    val = defaultdict(lambda : 0)
    for itm in inv["itms"] : 
       for key in  val_keys : 
          val[key] += itm["itm_det"][key]
    return val 
def json_generate(df,struct) :
   df = df.groupby(by=[struct[0]])
   data = []
   count = 0 
   for group_key,df in df :
     count += 1
     sub_data = {}      
     for key,func in struct[1].items() : 
         if  is_constant(func) : 
            if func == "count" : 
                sub_data[key] = count 
            else : 
                sub_data[key] = func
         elif type(func) == list : 
             sub_data[key] = json_generate(df,func)
         elif type(func) == dict :
             sub_data[key] = {}
             for key1,func1 in func.items() :
                 sub_data[key][key1] =  func1 if is_constant(func1) else func1(df,key1)
         else :
            sub_data[key] = func(df,key)
     data.append(sub_data)
   return data
def generate_b2b(df) :
  b2b = df[df.apply( lambda row : row["type"] == "b2b" and (  (not row["einvoice"]) or  row["override"] ) ,axis=1)]    
  struct = ["ctin",{"ctin": detail ,"inv":["inum",{ "inum": detail ,"val": detail ,"idt":detail ,"pos": "33","rchrg": "N","inv_typ": "R",
                                        "itms": ["rt",{"num":"count", "itm_det" : { "txval": sums , "rt": detail ,"iamt": 0,"camt": sums ,"samt": sums, "csamt": 0 }}] }] }]
  return json_generate(b2b,struct)
def generate_cdnr(df) :
 cdnr = df[df.apply( lambda row : row["type"] == "cdnr" and ( (not row["einvoice"]) or  row["override"] ) ,axis=1)]  
 cdnr = cdnr.rename(columns = {"idt" : "nt_dt","inum":"nt_num"})
 cdnr[["val","txval","camt","samt"]] = -cdnr[["val","txval","camt","samt"]]
 struct = ["ctin",{"ctin": detail ,"nt":["nt_num",{ "nt_num": detail ,"ntty": "C","val": detail ,"nt_dt":detail ,"pos": "33","rchrg": "N","inv_typ": "R",
                                        "itms": ["rt",{"num":"count", "itm_det" : { "txval": sums , "rt": detail ,"iamt": 0,"camt": sums ,"samt": sums, "csamt": 0 }}] }] }]
 return json_generate(cdnr,struct) 
def generate_b2c(df): 
  b2c = df[df["type"]=="b2cs"]
  struct = ["rt",{"txval": sums,"rt": detail ,"iamt": 0,"camt":sums,"samt": sums,"csamt": 0,"sply_ty": "INTRA","typ": "OE","pos": "33" }]
  return json_generate(b2c,struct) 
def generate_hsn(df) :
  struct = ["hsn_sc",{"num":"count","hsn_sc": detail ,"desc": detail ,"uqc": detail,"qty": sums ,
                  "txval": sums,"rt": detail ,"iamt": 0,"camt":sums,"samt": sums,"csamt": 0}]
  return {"data":json_generate(df[df["hsn_sc"].apply(lambda x : x==x and x is not None)],struct)}
def generate_docs(df) : 
    cutoff_invs = 20 #when to group them
    def group(df): 
     extra_docs = [] 
     for invs in list(set(df["docs"])) : 
         for inv in invs.split(",") : 
             extra_docs.append(inv) 
     l = list(set(list(df["inum"]) )) #if we want add extra_docs
     prefix = lambda string :  ''.join([ letter for letter in string  if letter.isalpha() ]) #the prefix finder function
     group = defaultdict(list)
     for inv in l : 
       if prefix(inv) != "" :
        group[prefix(inv)].append(inv) 
     
     idx = 1 
     docs = []
     for prefix, invs in group.items()  :
        if len(invs) > cutoff_invs :
            num = [ int("".join([ letter for letter in inv if letter.isnumeric() ]).lstrip("0"))   for inv in invs   ]
            num.sort() 
            invs.sort()
            tot = num[-1] - num[0] + 1
            if tot / len(invs) <= 1.5 :    
              docs.append({ "num": idx ,"to": invs[-1] ,"from": invs[0] ,
                 "totnum": tot ,"cancel": tot - len(invs)  ,"net_issue": len(invs) }) 
              idx += 1
              continue
        for inv in invs :
             docs.append({ "num": idx ,"to": inv ,"from": inv ,
                "totnum": 1 ,"cancel": 0  ,"net_issue": 1 })
             idx += 1
     return docs 
    outward = df[df["val"] >= 0 ]
    #outward["docs"] = ","
    cdnr = df[df["val"] < 0 ]
    data =[]
    docs = 	{"doc_det": data}
    structs =  [[1,"Invoices for outward supply",outward ] , [ 5, "Credit Note", cdnr]]
    for struct in structs : 
        data.append({"doc_num": struct[0],"doc_typ": struct[1],"docs":group(struct[2])})
    return docs 
						
def generate_json(cred,period) : 
    data = invoicedb.retriveinvs(cred,period)
    df = pd.DataFrame( data )
    gstin = gstsite.getuserdata(cred)["gstin"]
    _json = {"b2b": generate_b2b(df) , "cdnr" :generate_cdnr(df),"b2cs":generate_b2c(df),
           "hsn" : generate_hsn(df) , "doc_issue": generate_docs(df),"gstin": gstin, "fp": period , "version": "GST3.0.4", "hash": "hash"} 
    with open(f"{cred.user + period}.json","w+") as f :
         json_data = json.dumps(_json)
         json_data = json_data.replace("null",'"na"')
         json_data = json_data.replace("NaN",'"na"')
         f.write(json_data)
    return json_data
    
 
def summary_report(df) :
    value_summary = df.groupby(by="type",as_index=False).sum()[
                ["type","txval","camt","samt"]].round(2).to_dict(orient="records")
    count_summary = df.groupby(by="type",as_index=False)["inum"].nunique().to_dict(orient="records")
    count_summary.append({ "inum" : df["hsn_sc"].nunique() , "type" : "HSN"})  #doc needed to be added
    temp = {'type' :'total' ,"txval" : 0 , "camt" : 0 , "samt" : 0 }
    for key in ['txval','camt','samt']  :
        for i in value_summary : 
             temp[key] += i[key]
        temp[key] = round(temp[key],2)
    value_summary.append(temp)
    return value_summary , count_summary

def gstreportdata(cred,period) : 
    print(period)
    b2b = pd.DataFrame(gstsite.getinvs(cred,period,"b2b")).rename(columns = {"invcamt" : "camt"  ,"invsamt":"samt" ,"invtxval" : "txval"})
    cdnr = pd.DataFrame(gstsite.getinvs(cred,period,"cdnr")).rename(columns = {"invcamt" : "camt"  ,"invsamt":"samt" ,"invtxval" : "txval"})
    cdnr = cdnr.rename(columns = { "nt_num" : 'inum' }) 
    if 'camt' in cdnr.columns : 
       cdnr[['camt','samt','txval']] = -cdnr[['camt','samt','txval']]
    b2cs  = pd.DataFrame(gstsite.getinvs(cred,period,"b2cs")).rename(columns = {"invcamt" : "camt"  ,"invsamt":"samt" ,"invtxval" : "txval"})
    hsn =  pd.DataFrame(gstsite.getinvs(cred,period,"hsn"))
    data = { "B2B":b2b,"CDNR":cdnr,"B2CS":b2cs,"HSN":hsn}   
    for typ , datas in data.items() : 
        datas["filling_month"] =  period
        datas["type"] = typ 
    return  pd.concat([ value for value in data.values() ]) 
      
def gstreport(cred,periods) :
    data = pd.concat([ gstreportdata(cred,period) for period in periods ])
    for period in periods : 
       b2csa  = gstsite.getinvs(cred,period,"b2csa")
       for amend in b2csa : 
          inum = amend['pos']+"_"+amend["typ"]+"_"+amend["omon"]
          rt = pd.DataFrame(gstsite.getb2csa(cred,period,inum)).dropna()
          rt["type"] , rt["filling_month"] , rt["amended_month"] = "B2CSA" ,amend["omon"] , period 
          data = data[( (data["type"] != "B2CS") | (data["filling_month"]!=amend['omon']) )]
          data = pd.concat([data,rt])
    #data_formatted  = data.copy()
    #data_formatted["filling_month"] = data_formatted[""]
    summary = pd.pivot_table(data , values = ["txval","camt","samt"] , aggfunc = sum , index = ["filling_month","type"],
                             margins = True, margins_name='Total').reset_index().fillna(0)
    try : 
     rate = pd.pivot_table(data[data["type"] == "HSN"],index=["rt"] , values = ["txval","camt","samt"],aggfunc = sum).reset_index().fillna(0)
    except : 
        rate = pd.DataFrame()

    #modify = lambda df : pd.DataFrame ( df.reset_index().to_dict( orient = "split")["data"] , columns = [df ] ) 

    txval_table = pd.pivot_table(data , values = ["txval"] , columns = ["filling_month"] , aggfunc = sum , index = ["type"],
                             margins = True, margins_name='Total').reset_index().fillna(0)
    txval_table.columns = pd.Index([ col[1] for col in txval_table.columns ])
    
    camt_table = pd.pivot_table(data , values = ["txval"] , columns = ["filling_month"] , aggfunc = sum , index = ["type"],
                             margins = True, margins_name='Total').reset_index().fillna(0)
    camt_table.columns = pd.Index([ col[1] for col in camt_table.columns ])

    group = data.groupby(by="type")
    writer = pd.ExcelWriter(f"workings-{cred.user}-{periods[0]}.xlsx")
    invoicedb.addtable(writer = writer , sheet = "SUMMARY" , name = [ "SUMMARY","RATE" ,"TAXABLE","CGST"]  ,  data = [ summary , rate , txval_table,camt_table ] )  
    for typ,data in group : 
        invoicedb.addtable(writer = writer , sheet = typ.upper() , name = typ.upper() ,  data = data )  
    writer.save() 
    

    #data = {}
    #for period in periods : 
    #   data[period] =  gstreportdata(cred,period)

    #merge_data = {  typ : pd.concat([data[typ] for period,data in data.items()]) for typ in ["b2b","cdnr","b2cs","hsn"] }
    #summary = [dict( { "type":key }, **value.sum(axis = 0, skipna = True,numeric_only = True)) for key,value in  data.items() ]
    #summary = pd.DataFrame(summary)

def process_err(cred,period) : 
    errs = gstsite.process_err(cred,period)
    for err in errs["inactive"] : 
        buyer = invoicedb.getcoll(cred,period).find_one({"inum" : err["inum"]})["buyer"]
        err["buyer"] = buyer 
    return err 

def einvoice_compare(cred,period) : 
   data = invoicedb.retriveinvs(cred,period) 
   df = pd.DataFrame( data )
   if len(df.index) == 0 : 
       return "empty"
   gstb2b = gstsite.getinvs(cred,period,"b2b") 
   gstcdnr = gstsite.getinvs(cred,period,"cdnr") 
   for inv in gstcdnr : 
       inv['inum'] = inv.pop("nt_num")

   einvoices = pd.DataFrame(gstb2b + gstcdnr ,columns = ["inum","srctyp","type","invtxval","invcamt","invsamt"])
   einvoices = einvoices[einvoices["srctyp"] =='E-Invoice']
   einvoices = einvoices.rename(columns = {"invtxval" : "txval" , "invcamt" : "camt" , "invsamt" : "samt"})
   einvoices = einvoices[["inum","txval","camt","samt"]] 
   

   df_value =  df[df["type"]!="b2cs"].groupby(by="inum").sum()[["txval","camt","samt"]] 
   df_value["inum"] = df_value.index 
   df_value = df_value.reset_index(drop=True)
   

   #compare einvoices  and df_value and find mismatches . 
   lsuffix,rsuffix = "_source","_einvoice"
   e_mismatch = pd.merge( df_value , einvoices , how = "outer" , on="inum" , suffixes= (lsuffix,rsuffix) ) 
  
   e_logic = lambda row : any([ not(abs(abs(row[key+lsuffix]) - abs(row[key+rsuffix])) < 1)  for key in val_keys])
   e_diff = e_mismatch[e_mismatch.apply( lambda row : e_logic(row) , axis=1)] 
   e_correct_invs = list(set(e_mismatch[e_mismatch.apply(lambda row : not e_logic(row), axis=1)]["inum"] ))
   
   #change einvoice status 
   df["einvoice"] = df["inum"].apply( lambda inum : inum in e_correct_invs ) 
   invoicedb.change_einvoice_status(cred,period,{ inum : True  for inum in e_correct_invs })
   invoicedb.changestate(cred,period,{'einvoice_verification' : True})
    
   t1,t2 = set(df_value["inum"]) , set(einvoices["inum"]) 
   notfiled = list( (t1^t2)&t1 ) 

   einvoice_summary = {'Total Invoices' : df["inum"].nunique() , 'Total E-Invoices' : einvoices["inum"].nunique() , 
    'Missing E-Invoices' : len(notfiled) , 'Extra E-Invoices' : len(list( (t1^t2)&t2 )) , 
    'Conflicting E-Invoices' : len(e_diff.index) - len(notfiled)
   }
   
   removenan = lambda df :  df.fillna(0)
   mismatch = removenan(e_diff[e_diff["inum"].apply(lambda inum: inum not in notfiled)]).to_dict(orient="records") 
   notfiled = removenan(e_diff[e_diff["inum"].apply(lambda inum: inum in notfiled)])
   notfiled["idt"] = notfiled["inum"].apply(lambda inum : df[df["inum"] == inum].iloc[0]["idt"] )
   notfiled = notfiled.to_dict(orient="records")
   

   extras = list( (t1^t2)&t2 )
   process = einvoices[einvoices["inum"].apply(lambda inum :  inum in extras)]
   process["action"] = "ADDED"  
   process["remarks"] = "THEY ARE NOT IN SOURCE BUT THEY HAVE E-INVOICES "
   invoicedb.add_process(cred,period,process)

   value_summary , count_summary = summary_report( pd.concat([df,process]) )
   res = { "value_summary" : value_summary , "einvoice_summary" : einvoice_summary , "count_summary" : count_summary,
           "einvoice_mismatch" : mismatch, "einvoice_notfiled" :  notfiled }
   return res 
   #generate_json(cred,period)




   
   

    