from numpy import NaN
from conn import conn
import pandas as pd 
getcoll = lambda  cred,period  :  conn[cred.user][period]
def default_style(df) : 
     return  df.style.set_table_styles(
       [
         {
           'selector': 'th',
           'props': [
               ('background-color', 'blue'),
               ('color', 'White')]
         } 
       ])

def addtable(writer,sheet,name,data,style = "default") :
    def style(name,df) : 
        workbook  = writer.book
        worksheet = writer.sheets[sheet]
        merge_format = workbook.add_format({'bold': 1,'border': 1,'align': 'center', 'valign': 'vcenter','fg_color': 'yellow'})
        worksheet.merge_range(row-1,col,row-1,col+len(df.columns)-1,name,merge_format)
        
    if type(data) != list  : 
        data  = [data]
        name = [name]
    row = 2 
    col = 1 
    for i in range(0,len(data)) : 
       data[i] = data[i].dropna(axis='columns')
       data[i].to_excel(writer , sheet_name = sheet ,startrow  =  row , startcol = col  ,index =  ( True if type(data[i]) == pd.pivot_table else False )  )
       style(name[i] , data[i])
       row +=  3 + len(data[i].index)

def changestate(cred,period,states) : 
    coll = conn[cred.gstuser].states 
    coll.update_many({ "period" : period }, {"$set" : states } ,upsert = True )
def addinvs(cred,period,data) :
    coll = getcoll(cred,period)
    if type(data) == dict : 
       coll.insert_one(data)  
    else : 
      coll.insert_many(data)
def exist(cred,period) :
    return period in conn[cred.gstuser].list_collection_names()  
    
def reset(cred,period) : 
    getcoll(cred,period).delete_many({})
    getcoll(cred,period + "process").delete_many({})

def retriveinvs(cred,period , prefix = "") : 
    coll = getcoll(cred,period + prefix)
    return coll.find()



def change_einvoice_status(cred,period,data) : 
    coll = getcoll(cred,period) 
    for inum,status in data.items() : 
        coll.update_many({"inum" : inum }, {"$set" : { "einvoice"  :status }})
def add_process(cred,period,actions) :
     db = getcoll(cred,period + "process")
     actions =  pd.DataFrame(actions ,columns = ["idt","inum","action","from","to","txval","camt","samt","remarks","buyer"]).to_dict( orient="records")
     for action in actions : 
       db.update_one(action ,{ "$set" : action },upsert = True )
def mismatch_einvoice(cred,period) : 
     coll = getcoll(cred,period)
     coll.find_many({ "einvoice" : True , "override" : False  })

#x = [ { "ikeauser" : "SA"
#,"ikeapwd" : "Guna@@1973"
#,"dbName" : "41A210"
#,"baseurl" : "https://leveredge116.hulcd.com"
#,"clean" : False } , 
#  { "ikeauser" : "SA"
#,"ikeapwd" : "GUNa@@1973"
#,"dbName" : "413377"
#,"baseurl" : "https://leveredge35.hulcd.com"
#,"clean" : False }
#]

#db = conn["gst"]["gstsession"]
#db.update_many( {"user" :"GUNA"} , {"$set": {"data": x }} )


"""
 db = getcoll(cred,period + "process")
     actions =  pd.DataFrame(actions ,columns = ["idt","inum","action","from","to","txval","camt","samt","remarks"]).to_dict( orient="records")
     for action in actions :   
        prev =  db.find_one({"inum" : action["inum"] })
        if prev == None : 
            db.insert_one(action)
        else :
            prev["action"]
            db.insert_many(actions)
"""