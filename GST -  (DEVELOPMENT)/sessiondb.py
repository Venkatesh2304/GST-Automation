from conn import conn 

coll = conn.gst.gstsession
 
def addsession(cred,session) :  
    cookies = str([{'name': c.name, 'value': c.value, 'domain': c.domain, 'path': c.path}  for c in session.cookies])
    coll.update_many({"gstuser" : cred.gstuser}, {"$set" : {"cookies" : cookies}},upsert = True)
def retrivesession(cred,session) :
    cookies = eval( coll.find_one({"gstuser" : cred.gstuser})["cookies"] )
    for cookie in cookies : 
      session.cookies.set(**cookie)

