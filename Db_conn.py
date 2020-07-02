import pyodbc
import uuid
import pandas as pd
import csv
import json
import glob
import os

global tranId    
tranId = uuid.uuid4()

#reading the csv file
def reading_csv(filename):
    data=[]
    with open(filename,'r') as file:
        reader = csv.reader(file)
        for row in reader:
           data.append(row)
    return data 

#insert the headers into the table as columns
def headers(data,cursor,file):
    string="Values(" + ("'" +str(tranId)+"'"+",")
    for i in range(1,len(data[0])):
        s=(data[0][i])
        string+=("'" +s+"'"+",")
    
    json_file_name = os.path.splitext(file)[0] +".json"
    with open(json_file_name) as f_obj:
        js = json.load(f_obj)
    string+=("'" + json.dumps(js) + "'")
    # string = string[:-1]

    string+=')'
    # print(string)

    insertRecipient = "<Insert Statement>"
    insertRecipient+=(" "+string)
    # print(insertRecipient)
    # print(insertRecipient)
    cursor.execute(insertRecipient)
    cursor.commit()
    print("Insert success")

#insert the value into the table as columns
def values(data,cursor):
    string="Values(" + ("'" +str(tranId)+"'"+",")
    for i in range(1,len(data[3])):
        s=(data[3][i])
        string+=("'" +s+"'"+",")
    string = string[:-1]

    string+=')'
    # print(string)

    insertRecipient = "INSERT INTO <Insert Statement>)"
    insertRecipient+=(" "+string)
    # print(insertRecipient)
    cursor.execute(insertRecipient)
    cursor.commit()
    print("Insert success")

#insert the confidence into the table as columns
def confidence(data,cursor):
    string="Values(" + ("'" +str(tranId)+"'"+",")
    for i in range(1,len(data[6])):
        s=(data[6][i])
        string+=("'" +s+"'"+",")
    string = string[:-1]

    string+=')'
    # print(string)

    insertRecipient = "INSERT INTO <Insert Statement>)"
    insertRecipient+=(" "+string)
    # print(insertRecipient)
    cursor.execute(insertRecipient)
    cursor.commit()
    print("Insert success")


if __name__ == "__main__":
    conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};" 
                      "Server=<Your Server Name>"
                      "Database=<Your DB Name>;"
                      "uid=<Your ID>;pwd=<Your Password>")
    cursor = conn.cursor()
    print("Connected to Database")
    files = glob.glob(r'<File Name>')
    for file in files:
        data = reading_csv(file)
        headers(data,cursor,file)
        values(data,cursor)
        confidence(data,cursor)
    sql_query = pd.read_sql_query('SELECT * from <DB Name>',conn)
    print(sql_query)