# In order to simulate real-time data streaming,
# the dataset is split into smaller chunks of files which can be used in conjunction with the GetFile processor of Apache NiFi

csvfile = open('BigData/ecommerce-data541K.csv', 'r').readlines()

header = 'InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n'

filenamebase = 'DataFiles/DataFile'
fileNumber = 1

lineCountForEachFile = 10000

# Generate files
# Start with DataFile1 and create a new file with incremented suffix number for each 10K records
for i in range(1,len(csvfile)):
    if i % lineCountForEachFile == 0:
        filename = filenamebase + str(fileNumber) + '.csv'
        open(filename, 'w+').write(header)
        open(filename, 'a+').writelines(csvfile[i:i+lineCountForEachFile])
        fileNumber += 1
