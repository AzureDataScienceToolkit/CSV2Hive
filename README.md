# CSV2Hive
Python script and library that uses existing CSV file structure to automagically generate Hive CREATE TABLE command.

##Script syntax
<code>
 csv2hive.py [-h] [--output OUTPUT] [--noheader] [--colprefix COLPREFIX] [--csvserde] [--samplesize SAMPLE] table file

    positional arguments:
          table                 name of Hive table
          file                  location and name of CSV file

    optional arguments:
          -h, --help            show this help message and exit
          --output OUTPUT       location and name of the output file
           --noheader           if noheader is true, columns will be named automatically using prefix
                        
           --colprefix COLPREFIX Column prefix
                        
          --csvserde            use CSVSerde instead of plain text file
          --sample SAMPLE       number of rows to be sampled (default=10)
</code>



##Basic usage examples

### Generating Hive table structure using local CSV file

<code>
csv2hive.py mytable myfile.csv --output myscript.hql
</code>
### Using custom column names when headers are missing
Sometimes CSV file is not containing column headers. In such case we may use custom column naming convention, based on predefined column prefix and column number:

<code>
csv2hive.py mytable myfile.csv --noheader --colprefix "col"
</code>
###Defining the number of sampled rows
csv2hive.py will sample by default first ten rows of the file. It might happen that some of the columns are so sparse, it may not be possible to properly identify column type. We may extend the sampling range then:

<code>
csv2hive.py mytable myfile.csv --sample 100
</code>

###Using csvserde instead of plain text file
Some CSV files are more complex than others. They may use custom formatting rules like escaping , separators or quote characters. In such case it is best to switch from plain text processor to built-in CSV SerDe (warning: works with Hive 0.14+ only!):

<code>
csv2hive.py mytable myfile.csv --csvserde
</code>



