/*-- Load the unstructured data from HDFS */

raw_logs = LOAD 'hdfs://..../sample.log' USING TextLoader() as (line:chararray);

/* -- Filter logs based on info and warning

info = FILTER raw_logs BY (line matches '.*INFO.*');

warning = FILTER raw_logs BY (line matches '.*WARNING.*');

/*-- Extract useful info */

data = FOREACH info GENERATE SUBSTRING(line, 0, 28) AS date, SUBSTRING(line, 42, 46) AS pid, REGEX_EXTRACT(line, 'RT=\\[*([0-9]*)\\]*',1) AS RT, REGEX_EXTRACT(line, 'PT=\\[*([0-9]*)\\]*',1) AS PT, REGEX_EXTRACT(line, 'DBT=\\[*([0-9]*)\\]*',1) AS DBT;-

/*-- store the processed data */

STORE info into 'hdfs://10.0.7.243:8020/user/hdfs/NPCI/data.csv' using PigStorage(',');
