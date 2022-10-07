import mysql.connector
 


db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="sathya123",
  database="gst"
)
cursor = db.cursor()

def addsession(cred,session) :  
    cookies = str([{'name': c.name, 'value': c.value, 'domain': c.domain, 'path': c.path}  for c in session.cookies])
    query = f"""INSERT INTO gstsession (user,session) VALUES  (%s,%s)
      ON DUPLICATE KEY UPDATE session = %s"""
    cursor.execute(query,(cred.user,cookies,cookies))
    db.commit()
def retrivesession(cred,session) :
    cursor.execute(f"SELECT * FROM gstsession WHERE user = '{ cred.user }'")
    cookies = eval(cursor.fetchone()[1])
    for cookie in cookies : 
      session.cookies.set(**cookie)
def check_table(table_name) : 
   cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{check_table}'")
   return ( False if len(cursor.fetchall()) == 0 else True )
   return ( False if len(cursor.fetchall()) == 0 else True )
def check_db(db_name) : 
   cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_name = '{check_table}'")
   return ( False if len(cursor.fetchall()) == 0 else True )


def add_invoices(cred,period,data) : 
    CREATE DATABASE IF NOT EXISTS DBName;
    check_table( cred.user + "_" + period + "_" + "invoice" ) 
    

  



    

#def addsession(cred,data) : 
        


