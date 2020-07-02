import pyodbc

def write_file(data, filename):
    # Convert binary data to proper format and write it on Hard Disk
    with open(filename, 'wb') as file:
        file.write(data)

def readBLOB(name,bioData):
    print("Reading BLOB from python_employee table")
    try:
        conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};" 
                      "Server=<Your Server Name>"
                      "Database=<Your DB Name>;"
                      "uid=<Your ID>;pwd=<Your Password>")
        cursor = conn.cursor()
        sql_fetch_blob_query = """ SELECT * FROM <DB Name>
                          where pdf_Name    = ?"""
        cursor.execute(sql_fetch_blob_query, (name,))
        record = cursor.fetchall()
        # empPicture = convertToBinaryData(photo)
        for row in record:
            print("Name = ", row[0], )
            file = row[1]
            print("Storing employee image and bio-data on disk \n")
            # write_file(image, photo)
            write_file(file, bioData)
        # print("File inserted successfully as a BLOB into python_employee table", result)

    except conn.error() as error:
        print("Failed retrieving BLOB data into MySQL table {}".format(error))



readBLOB("FILE NAME",
           r"FILE PATH")