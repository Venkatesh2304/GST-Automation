from datetime import datetime
from flask import Flask , request , send_file
from flask.templating import render_template
from numpy import False_  
from werkzeug.exceptions import InternalServerError
import invoicedb
import sessiondb
import jsonmake
import gstsite
import hul
import pandas as pd
import json
import sys
#from flask_cors import CORS , cross_origin

app = Flask(__name__)

@app.errorhandler(InternalServerError)
def handle_500(e):
   exc_type, exc_value, tb = sys.exc_info()
   return str(exc_value) , 500

app.config['JSON_SORT_KEYS'] = False
class Credential() : 
     def __init__(self,dict = {}) :
        for key , value in dict.items() :  
         self.__setattr__(key,value)
     
get_cred = lambda req :   Credential(sessiondb.coll.find_one({"user" : req.cookies["user"] }))

def parse(request) :
    cred =  get_cred(request)
    data =  request.get_json()
    period = data["period"]
    return cred,data,period 

@app.route('/add',methods=["post"]) 
def add_invoices() : 
    cred =  get_cred(request)
    data =  request.get_json()
    period = data["period"]
         #clear invoices for the period
    inv_data = globals()[data["type"]].getinvoicedata(cred,period,invoicedb.add_process)
    #print(inv_data)
    invoicedb.addinvs(cred,period,inv_data)
    #invoicedb.changestate(cred,period,{"invoiceadded":True})
    return "true" 

@app.route('/getedit',methods =["post"])  
def edit() :
    cred = get_cred(request)
    period = request.get_json()["period"]
    invs  = pd.DataFrame( invoicedb.retriveinvs(cred,period,prefix = "process")  )
    invs["change"] = False 
    del invs["_id"]
    invs = invs.to_dict(orient="records")
    return invs

@app.route('/saveedit',methods =["post"])  
def sedit() : 
    cred = get_cred(request)
    period = request.get_json()["period"]
    changes = request.get_json()["changes"] 
    return changes 
    

@app.route("/reset",methods=["post"])
def reset() : 
    cred,data,period = parse(request)
    invoicedb.reset(cred,period)
    return "Done"

@app.route('/einvoicereport',methods=["post"]) 
def einvoice_report() : 
    cred =  get_cred(request)
    data =  request.get_json()
    period = data["period"]
    #invoicedb.changestate(cred,period,{"invoiceadded":True})
    return jsonmake.einvoice_compare(cred,period)

@app.route('/createjson',methods=["post"])
def createjson() : 
    cred,data,period = parse(request)
    jsonmake.generate_json(cred,period)
    return send_file(f"{cred.user + period}.json",cache_timeout=0)

@app.route('/periodexist',methods=["post"])
def period_is_exists(): 
    cred,data,period = parse(request)
    filed = jsonmake.getfiledstatus(cred,period)
    return filed 

@app.route('/gstreport',methods= ["post"])
def gstreport() : 
    cred,data,period = parse(request)
    dateparse = lambda d : datetime(int(d[2:]) , int(d[:2]),1)
    getperiodbtw = lambda m1,m2 :  [i.strftime("%m%Y") for i in pd.date_range(start=dateparse(m1), end=dateparse(m2), freq='MS')]  
    periods =  [period["fromd"]] + getperiodbtw(period["fromd"] , period["tod"])
    periods = list(set(periods))
    jsonmake.gstreport(cred,periods)
    return send_file(f"workings-{cred.user}-{periods[0]}.xlsx",as_attachment=True)
@app.route("/workingsreport",methods=["post"]) 
def workingsreport() : 
    cred,data,period = parse(request)
    
    invs = pd.DataFrame(invoicedb.retriveinvs(cred,period))
    tax_report = invs.groupby(by = ["rt"]).sum().reset_index()[["rt","txval","camt","samt"]]

    tables = data["tables"]
    writer = pd.ExcelWriter(f"workings-{cred.user}-{period}.xlsx")

    invoicedb.addtable( writer , "Summary" ,name = ["Summary","Count","Tax rate Breakup"] , data = [
      pd.DataFrame(tables["value_summary"]) ,  pd.DataFrame(tables["count_summary"]) , tax_report ] )

    changes  = invoicedb.getcoll(cred,period+"process").find({})
    invoicedb.addtable( writer , "Changes" ,name = "Changes" , data = [ pd.DataFrame(changes).fillna("-") ]) 
    invoicedb.addtable( writer , "einvoice" ,name = ["Summary","Mismatch","Missing"] , data = [
         pd.DataFrame([ {"TYPE":typ , "COUNT" : count} for typ,count in tables["einvoice_summary"].items() ]) 
      ,  pd.DataFrame(tables["einvoice_mismatch"]) ,  pd.DataFrame(tables["einvoice_notfiled"]) ] )
     
    invoicedb.addtable( writer , "Detailed" ,name = "Bill wise Breakup" , data = [ invs ]) 
    writer.save()
    return send_file(f"workings-{cred.user}-{period}.xlsx",as_attachment=True)

@app.route("/getrecaptcha",methods=["get"]) 
def getrecaptcha() :
    cred =  get_cred(request)
    gstsite.getcaptcha(cred)
    return send_file(f'{cred.user}-captcha.png',as_attachment=True)

@app.route("/sendcaptcha",methods=["post"]) 
def sendcaptcha() :
    cred =  get_cred(request)
    data =  request.get_json()
    gstsite.auth(cred,data["captcha"])
    return "Done"
        
@app.route("/upload",methods=["post"])
def upload() : 
    cred,data,period = parse(request)
    jsonmake.generate_json(cred,period)
    with open(f"{cred.user + period}.json","rb") as f : 
        return gstsite.upload(cred,period,f"{cred.user + period}.json" ,f.read() ) 

@app.route("/resolve",methods=["post"])
def resolve() :
    cred,data,period = parse(request)
    return gstsite.quickerr(cred,period,invoicedb)

@app.route("/correct",methods=["post"])
def correct() :
    cred,data,period = parse(request)
    return gstsite.correct(cred,period,data["corrected"],data["data"],invoicedb)

@app.route("/getconfig" , methods=["post"])
def getconfig() : 
    cred = sessiondb.coll.find_one({"user" : request.cookies["user"] })
    temp = {}
    for key in ["gstuser","gstpwd","pwd","data"] : 
        temp[key] = cred[key]
    cred = temp 
    cred.update(cred["data"])
    del cred["data"]
    return cred

@app.route("/setconfig" , methods=["post"])
def setconfig() : 
    cred =  get_cred(request)
    data =  request.get_json()["data"]
    temp = {} 
    for key in list(data.keys()) : 
       if  key not in ["gstuser","gstpwd","pwd"] : 
           temp[key] = data[key]
           del data[key] 
    data["data"] = temp 
    sessiondb.coll.update_one({ "user" : cred.user },{ "$set" : data })
    return "true"


app.run(threaded=True)  