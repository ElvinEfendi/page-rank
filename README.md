Page Rank algorithm using Hadoop
=========

This is one of my courseworks to get familiar with Map Reduce paradigm and Page Rank algorithm.

Jar is exported to the jar folder and also there are external library jsoup-1.7.3.jar inside libs folder.

To run the application:
hadoop jar jar/pageranker.jar org.pr.PageRanker -libjars libs/jsoup-1.7.3.jar arg1 arg2 arg3

arg1 - optional. this is the number of iteration to approximate page rank. default is 4
arg2 - optional. available values: both, ranking, parsing. both means run parsing, ranking(inluding sorting) jobs. ranking means run only ranking job. parsing means run only parsing job
arg2 - optional. input path of the data. default is hdfs://<host>:<port>/wikipedia/in

Final sorted pageranks will be in hdfs://<host>:<port>/wikipedia/ranking/ordered_result
