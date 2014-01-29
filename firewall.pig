A = LOAD 'hdfs://vm4.pal.com:8020/hdfs/z/firewall' using PigStorage('\t') as (mnth: chararray, day: chararray, time: chararray, devid: chararray, userid: chararray, intip: chararray, extip:chararray, srcport: chararray, destport: chararray, response: chararray);

--B = foreach A generate StringConcat(mnth, ' ', day, ' ', time);

--B = foreach A generate CONCAT(CONCAT(CONCAT(CONCAT(mnth, ' '), day), ' '), time)

B = foreach A generate StringConcat(mnth, ' ', day, ' ', time), REGEX_EXTRACT(devid, '^.*=(.*)$', 1),REGEX_EXTRACT(userid, '^.*=(.*)$', 1), REGEX_EXTRACT(intip, '^.*=(.*)$', 1), REGEX_EXTRACT(extip, '^.*=(.*)$', 1), REGEX_EXTRACT(srcport, '^.*=(.*)$', 1), REGEX_EXTRACT(destport, '^.*=(.*)$', 1), REGEX_EXTRACT(response, '^.*=(.*)$', 1);

STORE B into 'hdfs://vm4.pal.com:8020/hdfs/z/output' using PigStorage(',');

STORE B INTO 'hbase://firewall' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage ('f1: devid, f2: userid, f3: intip, f4: extip, f5:srcport, f6:destport, f7:response');
