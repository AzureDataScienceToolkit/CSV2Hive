import argparse, itertools, csv

class RemoteCSVFileReader:
    def __init__(self, filepath, samplesize, noheader, colprefix=None, row_preprocessor=None):
        self.sample_size = samplesize
        self.sample_rows = []
        self.header_row = []
        self._noheader = noheader
        self.col_prefix = colprefix
        if self._noheader and self.col_prefix is None:
            self.col_prefix = "c"
        if row_preprocessor is not None:
            self._preprocess = row_preprocessor

        if filepath.startswith("wasb:"): #WASB connection
            pass # TBD
        elif filepath.startswith("http"): #http URI connection
            pass # TBD
        elif filepath.startswith("hdfs"): #HDFS connection
            pass #TBD
        else: #local file
            self._parse_local_file(filepath)
        self.column_map = ColumnTypeSniffer(self).column_map

    def _parse_local_file(self, filepath):
        raw_file = open(filepath)
        if self._noheader is False:
            raw_header = [self._preprocess(raw_file.next())]
            self.header_row= csv.reader(iter(raw_header),delimiter=",").next()
        
        for line in range(self.sample_size):
            row = [self._preprocess(raw_file.next())]
            self.sample_rows.append(csv.reader(iter(row)).next())
        if self.col_prefix:
            self.header_row = [self.col_prefix+str(i) for i in range(len(self.sample_rows[0]))]

    def _preprocess(self, line):
        return line

class ColumnTypeSniffer:
    def __init__(self, remote_csv_file_reader):
        self.csv_reader = remote_csv_file_reader
        self.column_map = {}
        self._map_columns()
        self._parse_columns()

    def _map_columns(self):
        for colnum, colname in enumerate(self.csv_reader.header_row):
            self.column_map[colname] = [c[colnum] for c in self.csv_reader.sample_rows]

    def _parse_columns(self):
        for column in self.column_map:
            self.column_map[column] = self._sniff_column(self.column_map[column])

    def _sniff_column(self, samples):
        if self._validate_integer(samples):
            return 'int'
        elif self._validate_float(samples):
            return 'float'
        else:
            return 'string'

    def _validate_float(self, samples):
        try:
            [float(i) for i in samples]
            return True
        except:
            return False

    def _validate_integer(self, samples):
        try:
            [int(i) for i in samples]
            return True
        except:
            return False

class TableSchemaGenerator:
    def __init__(self, table, csv_reader, csvserde=False, header=False):
        self.hql_schema = []
        self.hql_schema.append("CREATE EXTERNAL TABLE %s (" % args.table)
        for column in csv_reader.header_row:
            self.hql_schema.append("\t %s %s," % (column, csv_reader.column_map[column]))
        #removing last comma
        self.hql_schema[-1] = self.hql_schema[-1][:-1]+")"

        if args.csvserde:
            self.hql_schema.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")
        else:
            self.hql_schema.append("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

        self.hql_schema.append("STORED AS TEXTFILE LOCATION \"\"")

        if not args.noheader: #skipping first row
                self.hql_schema.append ('tblproperties (\"skip.header.line.count\"=\"1\")')

        self.hql_schema[-1] = self.hql_schema[-1]+";"

    def write(self, filename):
        output = open(filename, "w")
        output.write('\n'.join(self.hql_schema))
        output.close()
    
    def dump(self):
        print '\n'.join(self.hql_schema)

def CustomRowPreprocessor(line):
    return line

#def CustomRowPreprocessor(line):
#    """Custom row preprocessor example"""
#    line = line.replace('"""', '""')
#    line = line.replace('""','"')[1:]
#    line = line.replace(';;','')
#    return line


if __name__ ==  "__main__":
    parser = argparse.ArgumentParser(description="Generates basic Hive table schema based on CSV file structure.")
    parser.add_argument('table', type=str, help='name of Hive table')
    parser.add_argument('file', type=str, help='location and name of CSV file')
    parser.add_argument('--output', type=str, default=None, help="location and name of the output file")
    parser.add_argument('--noheader', action="store_true", default=False, help='if noheader is true, columns will be named automatically using prefix')
    parser.add_argument('--colprefix', type=str, default=None, help='Column prefix')
    parser.add_argument('--csvserde', default=False, action="store_true", help="use CSVSerde instead of plain text file")
    parser.add_argument('--sample', type=int, default=10, help='number of rows to be sampled (default=10)')
    args = parser.parse_args()

    csv_structure = RemoteCSVFileReader(args.file, args.sample, args.noheader, colprefix=args.colprefix, row_preprocessor=CustomRowPreprocessor)
    schema_gen = TableSchemaGenerator(args.table, csv_structure, args.csvserde, args.noheader)
    if args.output:
        schema_gen.write(args.output)
    else:
        schema_gen.dump()






