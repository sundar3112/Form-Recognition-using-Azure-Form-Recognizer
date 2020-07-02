
import os
import json
import uuid
import requests
import random
import sys
import pyodbc 
import pandas as pd
import numpy as np
import json
import time
import pandas as pd
from requests import post,get
from pathlib import Path
attachment_dir = r'FILE PATH'
os.chdir(attachment_dir)


args = ['query']


def main(cursor):
    print("\nConnecting to GMail:...........")
    data = CheckforNewEmails(cursor)

def get_mostnew_email(messages):
    """
    Getting in most recent emails using IMAP and Python
    :param messages:
    :return:
    """
    ids = messages[0]  # data is a list.
    id_list = ids.split()  # ids is a space separated string
    #latest_ten_email_id = id_list  # get all
    latest_ten_email_id = id_list[-10:]  # get the latest 10
    keys = map(int, latest_ten_email_id)
    news_keys = sorted(keys, reverse=True)
    str_keys = [str(e) for e in news_keys]
    return  str_keys


def CheckforNewEmails(cursor):

    import imaplib, email, os
    import random
    import sys
    
    def Api_call(data_bytes,tranId):
        endpoint = r"Enter the Endpoint"
    apim_key = "Enter your API Key"
    model_id = "Enter your model ID"
        post_url = endpoint + "/formrecognizer/v2.0-preview/custom/models/%s/analyze" % model_id
        params = {
            "includeTextDetails": True
        }
        headers = {
            # Request headers
            'Content-Type': 'application/pdf',
            'Ocp-Apim-Subscription-Key': apim_key,
        }
        # with open(source, "rb") as f:
        #     data_bytes = f.read()

        try:
            resp = post(url = post_url, data = data_bytes, headers = headers, params = params)
            if resp.status_code != 202:
                print("POST analyze failed:\n%s" % json.dumps(resp.json()))
                # quit()
            print("POST analyze succeeded:\n%s" % resp.headers)
            get_url = resp.headers["operation-location"]
        except Exception as e:
            print("POST analyze failed:\n%s" % str(e))
            # quit() 

        n_tries = 15
        n_try = 0
        wait_sec = 5
        max_wait_sec = 60
        while n_try < n_tries:
            try:
                resp = get(url = get_url, headers = {"Ocp-Apim-Subscription-Key": apim_key})
                resp_json = resp.json()
                if resp.status_code != 200:
                    print("GET analyze results failed:\n%s" % json.dumps(resp_json))
                    # quit()
                status = resp_json["status"]
                if status == "succeeded":
                    print("Analysis succeeded:\n")
                    # with open(file,'w') as f_out:
                    #     json.dump(resp_json['analyzeResult']['documentResults'][0]['fields'],f_out)
                    resp_json_data = resp_json['analyzeResult']['documentResults'][0]['fields']
                    headers, values, confidences = jsonParse(resp_json_data)
                    print("JSON retrieval success!")

                    dbInsert(headers,values,confidences,cursor,resp_json['analyzeResult'],tranId,data_bytes)
                    
                    return 
                if status == "failed":
                    print("Analysis failed:\n%s" % json.dumps(resp_json))
                    quit()
                # Analysis still running. Wait and retry.
                time.sleep(wait_sec)
                n_try += 1
                wait_sec = min(2*wait_sec, max_wait_sec)     
            except Exception as e:
                msg = "GET analyze results failed:\n%s" % str(e)
                print(msg)
                quit()
        print("Analyze operation did not complete within the allocated time.")


    def dbInsert(headers, values, confidences,cursor,json_resp,tranId,data_bytes):
        headerInsert(headers,cursor,json_resp,tranId)
        valueInsert(values,cursor,tranId)
        confidenceInsert(confidences,cursor,tranId)
        insertFile(fileName, data_bytes)
        print("All values inserted successfully into DB!!!")


    user = '<USER EMAIL>'
    password = '<USER PASSWORD>'
    imap_url = 'imap.gmail.com'
    #Where you want your attachments to be saved (ensure this directory exists) 
    
    
    # sets up the auth
    def auth(user,password,imap_url):
        con = imaplib.IMAP4_SSL(imap_url)
        con.login(user,password)
        return con
    # extracts the body from the email
    def get_body(msg):
        if msg.is_multipart():
            return get_body(msg.get_payload(0))
        else:
            return msg.get_payload(None,True)
    
    # allows you to download attachments
    def get_attachments(part, ext, varSubject):       
        
        global fileName
        fileName = part.get_filename() 
        print(fileName)

        if bool(fileName):
            filePath = os.path.join(attachment_dir, fileName)
            data_bytes = part.get_payload(decode=True)
        return data_bytes
        

                
    #search for a particular email
    def search(key,value,con):
        result, data  = con.search(None,key,'"{}"'.format(value))
        return data
    #extracts emails from byte array
    def get_emails(result_bytes):
        msgs = []
        for num in result_bytes[0].split():
            typ, data = con.fetch(num, '(RFC822)')
            msgs.append(data)
        return msgs
    
    con = auth(user,password,imap_url)
    con.select('INBOX')
    
    result, data = con.search(None, '(UNSEEN)')
    
    if(b'' == data[0]) :
        print("\n \n NO NEW EMAIL.............!!")        
    else:
        new_emails = get_mostnew_email(data)  
        print("You have - " + str(len(new_emails)) + " new Email(s):........")
        #with open(attachment_dir + "log.txt", "a") as f:
           #f.write("You have - " + str(len(new_emails)) + " new Email(s):........\n")
        emailId = new_emails[0]

        for emailId in new_emails:                 
            result, data = con.fetch(emailId,'(RFC822)')
            msg = email.message_from_bytes(data[0][1]) 
            global varSubject
            rnd_subject = str(random.randint(1,999))
            varSubject = str(msg['subject'])
            varFrom = str(msg['from'])
            varSubject = ''.join(e for e in varSubject if e.isalnum())
            varSubject += '_' + rnd_subject
            print("Subject: " + varSubject)
            print("From: " + varFrom) 
            global InvoiceId
            global PortalId

            #Create Dir with varSubject
            # Path(attachment_dir+ "\\" + varSubject).mkdir(parents=True, exist_ok=True) 
            for part in msg.walk():
                if part.get_content_maintype()=='multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue

                rnd = str(random.randint(1,99999))
                data_bytes = get_attachments(part, rnd, varSubject)
                
                #calling the azure API to send the form and get the json back
                temp_file_name = os.path.splitext(fileName)[0]
                tranId = uuid.uuid4()
                Api_call(data_bytes,tranId)

                if(".pdf" in fileName.lower()):
                       print("\nConverting Pdf to Data................")
                #    print(fileName.lower())
                else:
                    print("file extenstion not supported: " + fileName.lower())

#parsing the Json data anf getting headers, data and confidences
def jsonParse(json_resp):
    df = pd.DataFrame(json_resp)
    # print(df)
    headers = list(df.columns)
    values=[]
    confidences = []

    # print(headers)
    for i in range(df.shape[1]):
        values.append(df.iloc[2,i])
        confidences.append(df.iloc[5,i])
    
    # print(len(headers), len(confidences), len(values))
   
    return headers,values,confidences


def headerInsert(headers,cursor,json_resp,tranId):
    string="Values(" + ("'" +str(tranId)+"'"+",")
    for header in headers:
        if(header==None):
            header=''
        string+=("'" + header +"'"+",")
    string+=("'" + json.dumps(json_resp) + "'")
    # string = string[:-1]

    string+=')'
    # print(string)

    insertRecipient = "<INsert Statement>"
    for i in range(1,len(headers)+1):
        s="[Col"+str(i)+"],"
        insertRecipient+=s
    insertRecipient += "[RawJson])"
    insertRecipient+=(" "+string)
    # print(insertRecipient)
    # print(len(headers),insertRecipient)
    cursor.execute(insertRecipient)
    cursor.commit()
    print("Insert success")

#insert the value into the table as columns
def valueInsert(values,cursor,tranId):
    string="Values(" + ("'" +str(tranId)+"'"+",")
    for value in values:
        if(value==None):
            value=''
        string+=("'" + value +"'"+",")
    string = string[:-1]

    string+=')'
    # print(string)

    insertRecipient = "<INsert Statement>"
    for i in range(1,len(values)+1):
        s="[Col"+str(i)+"],"
        insertRecipient+=s
    insertRecipient = insertRecipient[:-1] + ')'
    insertRecipient+=(" "+string)
    # print(len(values),insertRecipient)
    cursor.execute(insertRecipient)
    cursor.commit()
    print("Insert success")

#insert the confidence into the table as columns
def confidenceInsert(confidences,cursor,tranId):
    string="Values(" + ("'" +str(tranId)+"'"+",")
    for confidence in confidences:
        if(confidence==None):
            confidence=''
        string+=("'" + str(confidence) +"'"+",")
    string = string[:-1]

    string+=')'
    # print(string)

    insertRecipient = "<INsert Statement>"
    for i in range(1,len(confidences)+1):
        s="[Col"+str(i)+"],"
        insertRecipient+=s
    insertRecipient = insertRecipient[:-1] + ')'
    insertRecipient+=(" "+string)
    # print(len(confidences),insertRecipient)
    cursor.execute(insertRecipient)
    cursor.commit()
    print("Insert success")


def insertFile(fileName, data_bytes,cursor):
    try:
        sql_insert_blob_query = "<INsert Statement>"
        result = cursor.execute(sql_insert_blob_query, fileName,data_bytes)
        cursor.commit()
        print("File inserted successfully as a BLOB into table", result)

    except cursor.error() as error:
        print("Failed inserting BLOB data into MySQL table {}".format(error))
    
if __name__ == "__main__":
    import pyodbc 
    import pandas as pd
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};" 
                      "Server=<Your Server Name>"
                      "Database=<Your DB Name>;"
                      "uid=<Your ID>;pwd=<Your Password>")
    cursor = conn.cursor()
    main(cursor)
    conn.close()
    # print(fileName)
    






