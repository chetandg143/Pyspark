import smtplib

sender = 'chetan.goudar@tibilsolutions.com'
receivers = ['chetandg143@gmail.com']

message = "From: From Person <from@fromdomain.com>\
To: To Person <to@todomain.com>\
Subject: SMTP e-mail test"

try:
    smtpObj = smtplib.SMTP('localhost')
    smtpObj.sendmail(sender, receivers, message)
    print ("Successfully sent email")
except smtplib.SMTPException:
    print("Error: unable to send email")