/*
 * Created by Jesse Chen, IBM, jfchen@us.ibm.com, Feb 2015.
 * 
 * Loads an SQL.txt which has a TPCDS query and runs it in Spark using
 * hive context on pre-created hive tables, with data loaded. Outputs
 * query and execution time in seconds.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql.hive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import scala.io.Source._   

object TPCDSHiveOnSpark {
  case class Record(key: Int, value: String)

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TPCDSHiveOnSpark")
    val sc = new SparkContext(sparkConf)

    // Process program arguments and set properties
    if (args.length != 2 ) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <schema name> <fullpath of tpcds_query.txt>")
      System.exit(1)
    }

    // get and convert the input sql text file into sql string ready for use by hive
    val sqlText = fromFile(args(1)).getLines
    val fn = args(1).mkString
    // add a space because there is no guarantee the sql text has all the spacing and replace the last ;
    val sqlt = sqlText.mkString(" ").replace(";","")
    val schemaName = args(0).mkString

    // specifying a second parameter to the constructor. So tables created in Hive can be 
    // queried here.
    val hiveContext = new HiveContext(sc)
    import hiveContext._

    sql("set hive.security.authorization.enabled=false")
    // setup enough cached rows
    sql("set spark.sql.inMemoryColumnarStorage.batchSize=1000000000")
    // must have the following line to fix the parquet serde issue
    sql("set spark.sql.hive.convertMetastoreParquet=true")
    // sql("set spark.sql.codegen=true") this made performance worse
    sql("use "+ schemaName)

    // caching all the tables
    //hiveContext.cacheTable("tpcds100g.call_center")
    
    // timer wrapper to report query times; wrap around each sql call
    def time[A](f: => A) = {
 	val s = System.nanoTime
  	val ret = f
  	println("query time: "+(System.nanoTime-s)/1e9+" sec")
  	ret
    }
    
 
   // all hive TPCDS queries

	// query execution
	time { 
		sql(sqlt).collect().foreach(println) 
		println(fn + " completed") }

    sc.stop()

}
}
