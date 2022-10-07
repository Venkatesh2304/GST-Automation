import requests
import json
import sessiondb
import time
import os
import pandas as pd
import zipfile
import numpy as np
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Safari/537.36" }
def get(session,url,add_headers = {}) :
    _headers = headers.copy()
    _headers.update(add_headers)
    res =  session.get( url , headers = _headers )
    if res.status_code == 403 : 
        raise Exception("Gst Session Failed")
    else : 
       return res
def post(session,url,add_headers={},data = {} , files = {} , params=() ) :
    _headers = headers.copy()
    _headers.update(add_headers)
    res =  session.post(url,headers = _headers,data=data,files = files , *params)   
    if res.status_code == 403 : 
       raise Exception("Gst Session Failed")
    else : 
       return res
def getcaptcha(cred) : 
    session = requests.Session()
    get(session,'https://services.gst.gov.in/services/login')
    login = get(session, 'https://services.gst.gov.in/pages/services/userlogin.html')
    captcha = get(session,'https://services.gst.gov.in/services/captcha?rnd=0.7395713643528166')
    sessiondb.addsession(cred,session)
    with open(f'{cred.user}-captcha.png', 'wb') as f:
        f.write(captcha.content)
    return captcha.content 
def auth(cred,captcha) :
    session = requests.Session()
    sessiondb.retrivesession(cred,session)
    data =  { "captcha": captcha , "deviceID": None ,"mFP": "{\"VERSION\":\"2.1\",\"MFP\":{\"Browser\":{\"UserAgent\":\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.5005.115 Safari/537.36\",\"Vendor\":\"Google Inc.\",\"VendorSubID\":\"\",\"BuildID\":\"20030107\",\"CookieEnabled\":true},\"IEPlugins\":{},\"NetscapePlugins\":{\"PDF Viewer\":\"\",\"Chrome PDF Viewer\":\"\",\"Chromium PDF Viewer\":\"\",\"Microsoft Edge PDF Viewer\":\"\",\"WebKit built-in PDF\":\"\"},\"Screen\":{\"FullHeight\":864,\"AvlHeight\":816,\"FullWidth\":1536,\"AvlWidth\":1536,\"ColorDepth\":24,\"PixelDepth\":24},\"System\":{\"Platform\":\"Win32\",\"systemLanguage\":\"en-US\",\"Timezone\":-330}},\"ExternalIP\":\"\",\"MESC\":{\"mesc\":\"mi=2;cd=150;id=30;mesc=739342;mesc=770243\"}}" ,
            "password": cred.gstpwd , "type": "username" , "username": cred.gstuser }
    authenticate = post(session,"https://services.gst.gov.in/services/authenticate" ,add_headers = {'Content-type': 'application/json'},data = json.dumps(data))
    res = json.loads(authenticate.text)
    if "errorCode" in res.keys() : 
        if res["errorCode"] == "SWEB_9000" : 
           raise Exception("Invalid Captcha")
        elif res["errorCode"] == "AUTH_9002" : 
            raise Exception("Invalid Username or Password")
        elif res["errorCode"] == "AUTH_9033" : 
            raise Exception("Password Expired , kindly change password")
        else : 
            print(res)
            raise Exception("Unkown Exception")
    auth =  get(session ,"https://services.gst.gov.in/services/auth/",add_headers = {'Referer': 'https://services.gst.gov.in/services/login'})
    sessiondb.addsession(cred,session)
    return session 
def getb2csa(cred,period,inum) : 
    session = requests.Session()
    sessiondb.retrivesession(cred,session)
    return json.loads(get(session,f"https://return.gst.gov.in/returns/auth/api/gstr1/invoice?inum={inum}&rtn_prd={period}&sec_name=B2CSA&uploaded_by=OE",
                       add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr1"}).text) ["data"]["b2csa"][0]["itms"]
def getuserdata(cred) : 
    session = requests.Session()
    sessiondb.retrivesession(cred,session) 
    data = get(session,"https://services.gst.gov.in/services/api/ustatus",
           add_headers = {"Referer": "https://services.gst.gov.in/services/auth/fowelcome"})
    return json.loads(data.text) 

def getinvs(cred,period,types) :
    session = requests.Session()
    sessiondb.retrivesession(cred,session)
    uploaded_by = 'OE' if 'B2CS' in types.upper()  else 'SU'
    raw_invs = get(session,f"https://return.gst.gov.in/returns/auth/api/gstr1/invoice?rtn_prd={period}&sec_name={types.upper()}&uploaded_by={uploaded_by}",
                   add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr1"})
    data = json.loads(raw_invs.text)
    if "error" in data.keys()  :
        return []
    invs = data["data"]["processedInvoice"]
    return invs 
def getfiledstatus(cred,period) :
    session = requests.Session()
    sessiondb.retrivesession(cred,session) 
    res =  get(session,f"https://return.gst.gov.in/returns/auth/api/rolestatus?rtn_prd={period}",
                   add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr1"})
    data = json.loads(res.text)
    temp = { j['return_ty'] : j for i in data["data"]["user"] for j in i["returns"]  }
    return temp 
def process_err(cred,period,invoicedb) : 
     with zipfile.ZipFile(f"{cred.user}-error.zip") as zip_ref:
        with zip_ref.open(zip_ref.namelist()[0],mode='r') as file :
          err =  eval(file.read())
          errs = { "inactive" : [] ,"hsn" : [] }
          for typ in ['b2b','cdnr'] : 
              if typ in err['error_report'].keys() : 
                 for  ctin_err  in err['error_report'][typ] : 
                     if ctin_err["error_cd"] == "RET191360" : 
                         buyer = invoicedb.getcoll(cred,period).find_one({"ctin" : ctin_err["ctin"]})["buyer"]
                         errs["inactive"] += [  { "ctin" : ctin_err["ctin"] , "inum" :inum_err['inum'] ,"buyer" : buyer  }  for inum_err in ctin_err["inv"] ]  
          return errs 
    
def geterror(cred,period,ref_id,invoicedb) : 
    session = requests.Session()
    sessiondb.retrivesession(cred,session) 
    for times in range(0,20) : 
       time.sleep(1)
       res = get(session,f"https://return.gst.gov.in/returns/auth/api/offline/upload/summary?rtn_prd={period}&rtn_typ=GSTR1",
           add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"})  
       status_data = json.loads(res.text)["data"]["upload"]
       for status in status_data : 
           if status["ref_id"] == ref_id :
             print(status["er_status"])
             if status["er_status"] == "P" : 
               res = get(session,f"https://return.gst.gov.in/returns/auth/api/offline/upload/error/report/url?token={status['er_token']}&rtn_prd={period}&rtn_typ=GSTR1",
                     add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"}) 
               with open(f"{cred.user}-error.zip","wb") as f  : 
                     f.write(res.content) 
               return process_err(cred,period,invoicedb)
    return { "s" : False } 
def correct(cred,period,correct,data,invoicedb) : 
    coll = invoicedb.getcoll(cred,period)
    
    for inv in data :  
       if  inv["ctin"] in correct.keys()  and correct[inv["ctin"]]!="": 
         coll.update_many({ "inum" : inv["inum"] } , {"$set" : { "ctin" : correct[inv["ctin"]] }})
       else :  
         invs_data = pd.DataFrame(coll.find({"inum":inv["inum"]}))
         inv_data = [{ "buyer" : invs_data["buyer"].iloc[0] , "idt" :  invs_data["idt"].iloc[0] ,"inum" : inv["inum"],
         "txval" : invs_data.sum()["txval"] , "camt" : invs_data.sum()["camt"] , "samt" : invs_data.sum()["samt"] , 
         "from" :"B2B","to":"B2C","remarks": "WRONG GST NUMBER . SO CONVERTED TO B2C" ,"action":"CHANGE TYPE" }]
         

         invoicedb.add_process(cred,period,inv_data)

         coll.update_many({ "inum" : inv["inum"] } , {"$set" : { "ctin" : np.NAN , "type" : "b2cs"}}) 
    return "sdfsd"    

def quickerr(cred,period,invoicedb) : 
   gstb2b = getinvs(cred,period,"b2b") 
   gstcdnr = getinvs(cred,period,"cdnr") 
   for inv in gstcdnr : 
       inv['inum'] = inv.pop("nt_num")
   invs_data = pd.DataFrame(invoicedb.retriveinvs(cred,period))
   invs_data = invs_data[invs_data["type"] != "b2cs"]
   invs =  set(invs_data["inum"])
   errs = ( invs ^ set([ i["inum"] for i in   gstb2b + gstcdnr ]) ) & invs 
   errs = list(errs) 
   errs =  invs_data[invs_data["inum"].apply( lambda inum : inum in errs )][["ctin","inum","buyer"]]
   errs = errs.drop_duplicates()
   #temp = errs[["ctin","buyer"]].drop_duplicates()

   return { "inactive" :  errs.to_dict(orient = "records" )  } 
def upload(cred,period,fname , binary) :
    start = time.time() 
    session = requests.Session()
    sessiondb.retrivesession(cred,session) 

    temp = get(session,"https://services.gst.gov.in/services/api/ustatus",
           add_headers = {"Referer": "https://services.gst.gov.in/services/auth/fowelcome"})
    temp = json.loads(temp.text)
    
    input(temp["bname"])
    files = {'upfile': ( fname ,binary , 'application/json', { 'Content-Disposition': 'form-data' })}

    res =  post(session,f"https://return.gst.gov.in/returndocs/offline/upload",
           add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload" , "sz" : "304230" }, 
           data = {  "ty": "ROUZ" , "rtn_typ": "GSTR1" , "ret_period": period } ,files=files)
    ref_id = json.loads(res.text)['data']['reference_id']

    res = post(session, "https://return.gst.gov.in/returns/auth/api/gstr1/upload" , data = json.dumps({"status":"1","data":{"reference_id":ref_id},"fp":period}),
         add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload" , "Content-Type": "application/json;charset=ISO-8859-1"}) 

    for times in range(0,90) : 
       time.sleep(1)
       res = get(session,f"https://return.gst.gov.in/returns/auth/api/offline/upload/summary?rtn_prd={period}&rtn_typ=GSTR1",
           add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"})  
       status_data = json.loads(res.text)["data"]["upload"] 
       for status in status_data : 
           if status["ref_id"] == ref_id : 
              if status["status"] == "P"  : 
                  return { "status" :  True } 
              if status["status"] == "PE" : 
                  get(session,f" https://return.gst.gov.in/returns/auth/api/offline/upload/error/generate?ref_id={ref_id}&rtn_prd={period}&rtn_typ=GSTR1",
                         add_headers = {"Referer": "https://return.gst.gov.in/returns/auth/gstr/offlineupload"})
                  return  { "status" :  False , "resolve" : True ,"ref_id" : ref_id  } 
              if status["status"] == "ER" : 
                  return { "status" :  False , "resolve" : False  ,"err" : status["er_msg"] } 
    
    
              





