import csv
import gzip
from lib2to3.pgen2.token import NEWLINE
import time
import sys
from pathlib import Path

# source from https://github.com/gdmcdonald/json2csv
# someone create a script for converting json to csv file, so to solve the memory problem

#input .json.gz file, in the same folder as this python script.
inputFileName=sys.argv[1]

#remove last 8 charachters (".json.gz") to get file name without extentions
fileNameRoot=Path(inputFileName).stem
fileNameRoot=Path(fileNameRoot).stem

#max lines to output in a single csv file
if (len(sys.argv)>2):
    linesPerOutputFile= int(sys.argv[2])
else:
    linesPerOutputFile=1000000

#hardcoded field names.....sob
fieldnames=['image','overall', 'vote', 'verified', 'reviewTime', 'reviewerID', 'asin', 'style', 'reviewerName', 'reviewText', 'summary', 'unixReviewTime']

#initialize the output file number at zero
fileNumber=0

#count lines and read in field names on first pass
with gzip.open (inputFileName,'r') as jsonfile:
    totalLinesInInputFile=0
    t0 = time.time()
    for line in jsonfile:
        true=True
        false=False
        lineDictionary=eval(line)
        totalLinesInInputFile+=1
        break
    #fieldnames=[key for key, value in lineDictionary.items()]
    #this makes the fields save in random order in the output csv, let's go back to hard coded
    for line in jsonfile:
        totalLinesInInputFile+=1
    t1 = time.time()
    #print(str(t1-t0)+" seconds")
    print(str(totalLinesInInputFile)+" lines to convert to csv.")


#convert to multiple csv files on second pass
with gzip.open (inputFileName,'r') as jsonfile:

    while 1 == 1:
        fileNumber += 1
        outputFileName = fileNameRoot + str(fileNumber) + '.csv.gz'
        countOfLinesInThisFile = 0
        linesDoneSoFar = countOfLinesInThisFile + (fileNumber - 1) * linesPerOutputFile
        if linesDoneSoFar < totalLinesInInputFile:
            with gzip.open(outputFileName, 'wt', newline='') as csvfile:
                OutputCsvFileWriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
                OutputCsvFileWriter.writeheader()

                t0 = time.time()
                for line in jsonfile:
                    #evaluate the line of json, which will make it a python dictionary.
                    lineDictionary=eval(line)
                    OutputCsvFileWriter.writerow(lineDictionary)
                    countOfLinesInThisFile+=1
                    linesDoneSoFar = countOfLinesInThisFile + (fileNumber - 1) * linesPerOutputFile
                    print(str(linesDoneSoFar) + " lines converted, %0.0f" % (linesDoneSoFar / totalLinesInInputFile * 100) + "% done.", end='\r')
                    if countOfLinesInThisFile == linesPerOutputFile:
                        break
                t1 = time.time()
                print("File number " + str(fileNumber) + " took %.2f" % (t1-t0) + " seconds.                                ")
        else:
            break