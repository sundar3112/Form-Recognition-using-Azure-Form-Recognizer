import pyodbc

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def insertBLOB(name, biodataFile):
    print("Inserting BLOB into python_employee table")
    try:
        conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};" 
                      "Server=<Your Server Name>"
                      "Database=<Your DB Name>;"
                      "uid=<Your ID>;pwd=<Your Password>")
        cursor = conn.cursor()
        sql_insert_blob_query = """ <INSERT STATEMENT>"""

        # empPicture = convertToBinaryData(photo)
        file = convertToBinaryData(biodataFile)

        # Convert data into tuple format
        insert_blob_tuple = (name, file)
        result = cursor.execute(sql_insert_blob_query, name,file)
        cursor.commit()
        print("File inserted successfully as a BLOB into python_employee table", result)

    except conn.error() as error:
        print("Failed inserting BLOB data into MySQL table {}".format(error))



insertBLOB("FILE NAME",
           r"FILE PATH")