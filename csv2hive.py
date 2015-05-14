import sys
import urllib2
import csv
import datetime
import time

def validateYYMMDD(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
	return 1
    except ValueError:
	return 0

def validateDDMMYY(date_text):
    try:
        datetime.datetime.strptime(date_text, '%d/%m/%y')
	return 1
    except ValueError:
	return 0

def validatefloat(text):
    try:
        float(text)
	return 1
        return float
    except ValueError:
	return 0

def validateHHMM(date_text):
    try:
        time.strptime(date_text, '%H:%M')
	return 1
    except ValueError:
	return 0

def coltype( arr, coln ):
 col_int = 1
 col_float = 1
 col_DT = 1
 col_DATE = 1
 col_DDMMYY = 1
 col_YYMMDD = 1
 col_HHMM = 1
 for n in arr :
   ct = unicode(n[coln], 'utf-8')
   if ct.isnumeric() == 0 :
 	col_int = 0
   if validatefloat(ct) == 0:
 	col_float = 0
   if validateDDMMYY(ct) == 0:
 	col_DDMMYY = 0
   if validateYYMMDD(ct) == 0:
 	col_YYMMDD = 0
   if validateHHMM(ct) == 0:
 	col_HHMM = 0


 RS = "string"
 if col_float == 1 :
   RS = "float"
 if col_int == 1 :
   RS = "int"
 if col_DDMMYY == 1 :
   RS = "string"

 if col_YYMMDD == 1 :
   RS = "string"

 if col_HHMM == 1 :
   RS = "string"

 return RS

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: GenHiveCreate Table STORAGEURL HEAD/NOHEAD SAMPLESIZE")
        exit(-1)

    HEAD = 0
    n = 10
    file_name = sys.argv[1]
    if sys.argv[2] == "HEAD" :
       HEAD = 1
    n = int(sys.argv[3])
    nlines = 0


    url = sys.argv[1]
    data = urllib2.urlopen(url)
    cr = csv.reader(data)
    HEADS = []
    MAXC = 0
    MINC = 9999999
    HC = 0
    result = []
    WASBI = url.split('/');
    WASB = "wasb://{}@{}/".format(WASBI[3],WASBI[2])
    #for WI in range(4, len(WASBI)):
    #   WASB = "{}/{}".format(WASB,WASBI[WI])
    TN = WASBI[len(WASBI) -1]
    TNA = TN.split('.')
    TN = TNA[0]


    for row in cr:
       if HEAD == 1 and nlines == 0:
         for i in row:
            HEADS.append(i)
            HC += 1
       else :
         result.append(row)
	 if len(row) > MAXC :
	  MAXC=len(row)
	 if len(row) < MINC :
	  MINC=len(row)

       nlines += 1
       if nlines >= n:
            break

    if nlines > 1 :
       if HEAD == 1:
         if MAXC > HC :
           print "WARNING: Some Row columns > Header columns"
         if MINC != HC :
           print "WARNING: Some Row columns are < Header columns"

       print "DROP TABLE {};".format(TN)
       print "CREATE EXTERNAL TABLE {}".format(TN)
       print "("
       if HEAD == 1 :
         cn = 0
         for HX in HEADS :
		cn = cn+1
		cs = coltype(result,cn-1)
                if cn < len(HEADS):
		  print "{} {} ,".format(HX,cs)
		else :
		  print "{} {}".format(HX,cs)
       else :
         for HI in range(1, MAXC):
		cs = coltype(result,HI-1)
                if HI < MAXC:
		  print "COL{} {},".format(HI,cs)
		else:
		  print "COL{} {}".format(HI,cs)

       print ")"
       print "COMMENT \"{}\"".format(url)
       print "ROW FORMAT   DELIMITED"
       print "FIELDS TERMINATED BY ','"
       print "STORED AS TEXTFILE LOCATION '{}'".format(WASB)
       if HEAD == 1:
         print "tblproperties (\"skip.header.line.count\"=\"1\"); "
