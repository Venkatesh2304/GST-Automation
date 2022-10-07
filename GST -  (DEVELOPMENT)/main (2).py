import os 
import ssl
import smtplib
from os.path import basename
from email.message import EmailMessage
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
sender = "automategst@gmail.com"
pwd = "kiglcvdfftvovjlr"

def mail(cred,period,fpath,subject,body="") :  
  to = cred 
  text = f"""
    <html>
    <body > 
    <p style="'color':'red'"> THIS IS A AUTO GENERATED EMAIL   </p>
    {body}
    Regards , 
      <p> ~~Venkatesh </p> , 
      ~~AUTOMATE GST . 
    Contact : gstautomate@gmail.com , venkateshks2304@gmail.com
    </body>
    </html>
  """
  msg = MIMEMultipart()
  msg["From"] = sender 
  msg["To"] = to
  msg["Subject"] = subject 
  msg.attach(MIMEText(text,"html"))

  with open(fpath, "rb") as file :
        part = MIMEApplication(file.read(),Name=basename(fpath))
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(fpath)
        msg.attach(part)
  #em.set_content(body)
  context = ssl.create_default_context()
  with smtplib.SMTP_SSL("smtp.gmail.com",465,context=context) as smtp : 
       smtp.login(sender,pwd) 
       smtp.sendmail(sender,to,msg.as_string())
  return True 
mail("sathish1974@gmail.com,venkateshks2304@gmail.com","July",r"workings-DEVAKI-012022.xlsx","GSTR1 July 2022 DEVAKI PREVIEW REPORT")
print("ASD")