Running TPCDS queries on Spark
```
You can submit a Spark SQL job that accesses the existing Hive metastore; once there, you
will be able to run queries against your existing Hive tables. This Scala source allows
you run 38 out of the 99 TPCDS queries (The Hive version supported by Spark SQL in the 
current release of IBM Open Platform available at
http://g01zcdwas002.ahe.pok.ibm.com/software/data/infosphere/hadoop/trials.html
is 0.13.1; the Hive version shipped in IBM Open 
Platform 4.0.0 is 0.14.0. Any new features introduced in Hive 0.14.0 will not be supported. 
There are documented unsupported features for Hive support. Refer to the 
Spark online documentation for more detailed information.)

The list of TPCDS queries you can use to test is as follows:

          898 Apr 14 09:17 query97.sql
          451 Apr 14 09:17 query96.sql
         1476 Apr 14 09:17 query95.sql
         1021 Apr 14 09:17 query94.sql
         1135 Apr 14 09:17 query91.sql
         1071 Apr 14 09:17 query90.sql
         5644 Apr 14 09:17 query88.sql
         1158 Apr 14 09:17 query87.sql
         2234 Apr 14 09:17 query85.sql
          739 Apr 14 09:17 query84.sql
          532 Apr 14 09:17 query82.sql
          949 Apr 14 09:17 query79.sql
         1258 Apr 14 09:17 query76.sql
         1216 Apr 14 09:17 query73.sql
         1738 Apr 14 09:17 query71.sql
         1416 Apr 14 09:17 query68.sql
         8366 Apr 14 09:17 query66.sql
         2274 Apr 14 09:17 query58.sql
          384 Apr 14 09:17 query55.sql
          472 Apr 14 09:17 query52.sql
         1594 Apr 14 09:17 query50.sql
         1211 Apr 14 09:17 query48.sql
         1365 Apr 14 09:17 query46.sql
         1043 Apr 14 09:17 query43.sql
          439 Apr 14 09:17 query42.sql
          931 Apr 14 09:17 query40.sql
         1878 Apr 14 09:17 query39.sql
         1404 Apr 14 09:17 query34.sql
         1113 Apr 14 09:17 query32.sql
          744 Apr 14 09:17 query26.sql
          986 Apr 14 09:17 query21.sql
          775 Apr 14 09:17 query19.sql
         1326 Apr 14 09:17 query18.sql
         1827 Apr 14 09:17 query17.sql
          610 Apr 14 09:17 query15.sql
         1426 Apr 14 09:17 query13.sql
          727 Apr 14 09:17 query07.sql
          466 Apr 14 09:17 query03.sql

Setup

We assume you have a TPCDS database, have set up TPCDS tables and have loaded data.
If not, please refer to the TPCDS benchmark site for more information 
http://www.tpc.org/tpcds/
on data generation and DDLs for database creation. At the end of the process, 
you should see something similar to the following output in the Hive shell:

	Logging initialized using configuration in file:/etc/hive/conf/hive-log4j.properties
	hive> use tpcds1000g;
	OK
	Time taken: 1.406 seconds
	hive> show tables;
	OK
	call_center
	catalog_page
	catalog_returns
	catalog_sales
	customer
	customer_address
	customer_demographics
	date_dim
	household_demographics
	income_band
	inventory
	item
	promotion
	reason
	ship_mode
	store
	store_returns
	store_sales
	time_dim
	warehouse
	web_page
	web_returns
	web_sales
	web_site
	Time taken: 0.098 seconds, Fetched: 24 row(s)


Build the Scala app

	cd <dir containing 'src' and 'build.sbt'>
	.../bin/sbt package

	This will generate a jar file named:

	target/scala-2.10/tpcdshiveonspark_2.10-1.2.1.jar


Running a TPCDS query in Spark


	/usr/bin/spark-submit  --master yarn-client \
	--files /usr/iop/4.0.0.0/spark/conf/hive-site.xml \
	--name TPCDSHiveOnSpark 
	--executor-memory 4096m 
	--num-executors 100 
	--jars 	/usr/iop/4.0.0.0/spark/lib/datanucleus-core-3.2.10.jar,\
		/usr/iop/4.0.0.0/spark/lib/datanucleus-api-jdo-3.2.6.jar,\
		/usr/iop/4.0.0.0/spark/lib/datanucleus-rdbms-3.2.9.jar \
	--class org.apache.spark.examples.sql.hive.TPCDSHiveOnSpark \
	/TestAutomation/sbt/target/scala-2.10/tpcdshiveonspark_2.10-1.2.1.jar \
	tpcds1000g /TestAutomation/sbt/src/main/hive-queries/query91.sql

	The command above is formatted for readability. It should be one line without the '\' character.

Output:

	15/04/14 09:52:35 INFO scheduler.DAGScheduler: Job 5 finished: collect at SparkPlan.scala:84, took 0.768780 s
	[AAAAAAAAACAAAAAA,California_2,Jason Johnson,69308.28999999998]
	[AAAAAAAAACAAAAAA,California_2,Jason Johnson,58897.1]
	[AAAAAAAAJBAAAAAA,NY Metro_2,Richard James,54159.950000000004]
	[AAAAAAAAKBAAAAAA,Mid Atlantic_2,Rene Sampson,51577.48999999999]
	[AAAAAAAADBAAAAAA,Pacific Northwest_1,Roderick Walls,51380.939999999995]
	[AAAAAAAAIAAAAAAA,California,Wayne Ray,50627.3]
	[AAAAAAAAKBAAAAAA,Mid Atlantic_2,Rene Sampson,49320.490000000005]
	[AAAAAAAACAAAAAAA,Mid Atlantic,Felipe Perkins,48337.630000000005]
	[AAAAAAAAFCAAAAAA,NY Metro_3,Marshall Pate,45954.64]
	[AAAAAAAAGCAAAAAA,Mid Atlantic_3,Michael Morrison,45613.4]
	[AAAAAAAANAAAAAAA,NY Metro_1,Jack Little,45343.75]
	[AAAAAAAAPBAAAAAA,Pacific Northwest_2,Shane Crews,44978.56999999999]
	[AAAAAAAAHAAAAAAA,Pacific Northwest,Alden Snyder,44752.76]
	[AAAAAAAAEAAAAAAA,North Midwest,Larry Mccray,43648.009999999995]
	[AAAAAAAAICAAAAAA,North Midwest_3,Edgar Tate,39240.490000000005]
	[AAAAAAAAICAAAAAA,North Midwest_3,Edgar Tate,38945.98000000001]
	[AAAAAAAAGBAAAAAA,Hawaii/Alaska_1,Travis Wilson,38502.56]
	[AAAAAAAAMBAAAAAA,North Midwest_2,Ryan Burchett,37428.400000000016]
	[AAAAAAAAIAAAAAAA,California,Wayne Ray,36265.520000000004]
	[AAAAAAAABAAAAAAA,NY Metro,Bob Belcher,35411.91]
	[AAAAAAAAMBAAAAAA,North Midwest_2,Ryan Burchett,34860.57]
	[AAAAAAAAGBAAAAAA,Hawaii/Alaska_1,Travis Wilson,34733.13]
	[AAAAAAAACAAAAAAA,Mid Atlantic,Felipe Perkins,33816.119999999995]
	[AAAAAAAAPBAAAAAA,Pacific Northwest_2,Shane Crews,32988.5]
	[AAAAAAAABAAAAAAA,NY Metro,Bob Belcher,32825.28999999999]
	[AAAAAAAANAAAAAAA,NY Metro_1,Jack Little,32821.149999999994]
	[AAAAAAAACCAAAAAA,Hawaii/Alaska_2,Kendall Jones,30901.54]
	[AAAAAAAAKAAAAAAA,Hawaii/Alaska,Gregory Altman,30840.999999999996]
	[AAAAAAAAABAAAAAA,North Midwest_1,Timothy Bourgeois,30318.090000000004]
	[AAAAAAAAKAAAAAAA,Hawaii/Alaska,Gregory Altman,30243.050000000003]
	[AAAAAAAAOAAAAAAA,Mid Atlantic_1,Clyde Scott,29703.32]
	[AAAAAAAAGCAAAAAA,Mid Atlantic_3,Michael Morrison,28091.28]
	[AAAAAAAAFCAAAAAA,NY Metro_3,Marshall Pate,27094.789999999994]
	[AAAAAAAADBAAAAAA,Pacific Northwest_1,Roderick Walls,26577.600000000002]
	[AAAAAAAAEBAAAAAA,California_1,Jason Brito,26463.740000000005]
	[AAAAAAAAEBAAAAAA,California_1,Jason Brito,26260.539999999997]
	[AAAAAAAAJBAAAAAA,NY Metro_2,Richard James,25902.109999999997]
	[AAAAAAAAHAAAAAAA,Pacific Northwest,Alden Snyder,25043.189999999995]
	[AAAAAAAACCAAAAAA,Hawaii/Alaska_2,Kendall Jones,25017.15]
	[AAAAAAAAOAAAAAAA,Mid Atlantic_1,Clyde Scott,23325.43]
	[AAAAAAAAEAAAAAAA,North Midwest,Larry Mccray,21593.25]
	[AAAAAAAAABAAAAAA,North Midwest_1,Timothy Bourgeois,21558.3]
	/TestAutomation/sbt/src/main/hive-queries/query91.sql completed
	query time: 27.483025633 sec
```
