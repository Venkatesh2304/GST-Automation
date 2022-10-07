import requests
import pandas as pd 
from datetime import datetime 
from dateutil.relativedelta import relativedelta
def excel_download(data,period) :
   session = requests.Session()
   date = lambda : int((datetime.now() - datetime(1970,1,1)).total_seconds() *1000) - (330*60*1000) 
   _data  = {'userId': data["ikeauser"] , 'password': data["ikeapwd"] , 'dbName': data["dbName"] , 'datetime': date() , 'diff': -330 }
   print(session.post( data["baseurl"] + '/rsunify/app/user/authentication.do',data=_data).text)
   session.post( data["baseurl"] + "/rsunify/app/user/authenSuccess.htm")
   fromd = datetime(int(period[2:]), int(period[:2]) ,1 )
   tod = fromd  + relativedelta(day  = 31)
   _period = "/"+ period[:2] + "/" + period[2:] 
   dpath = session.post(data["baseurl"] + f'/rsunify/app/gstReturnsReport/gstReturnReportGenerate?pramFromdate={fromd.strftime("%d/%m/%Y")}&paramToDate={tod.strftime("%d/%m/%Y")}&gstrValue=1&paramId=2')
   response = session.get(data["baseurl"] + "/rsunify/app/reportsController/downloadReport?filePath=" + dpath.text)
   return response.text  
