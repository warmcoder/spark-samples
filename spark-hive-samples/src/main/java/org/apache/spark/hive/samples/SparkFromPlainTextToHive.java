/*
 * Licensed to spark-samples under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. spark-samples licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.hive.samples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.hive.samples.common.Employee;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @author nabil
 */
public class SparkFromPlainTextToHive {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// Creating a Spark session ...

		SparkSession sparkSession = SparkSession.builder().appName("org.apache.spark.nabilexamples").master("local")
				.config("spark.cores.max", "10").enableHiveSupport().getOrCreate();

		// Creating RDD of formatted data ...

		JavaRDD<Employee> employeeRDD = sparkSession.sparkContext()
				.textFile("hdfs://localhost:8020/user/nabil/employee.txt", 1).toJavaRDD()
				.map(new Function<String, Employee>() {

					public Employee call(String line) throws Exception {

						final String[] data = line.split(",");
						return new Employee(data[0].trim(), Integer.valueOf(data[1].trim()), data[2].trim());
					}

				});

		// Converting RDD to DataSet ...
		
		Dataset<Row> employeeDS = sparkSession.createDataFrame(employeeRDD, Employee.class);

		// Creating temp view for queries ...

		employeeDS.createOrReplaceTempView("employee_view");

		// Querying ...

		Dataset<Row> youngPeople = sparkSession.sql("SELECT name, age FROM employee_view WHERE age BETWEEN 18 AND 30");

		// Show the results of previous query ...
		
		youngPeople.show();

		// Storing results in Hive ...
		
		sparkSession
				.sql("CREATE TABLE IF NOT EXISTS employee (name String, age int) COMMENT 'Employee between 18 and 30'");

		sparkSession.sql("INSERT INTO TABLE employee SELECT name, age from employee_view");

		sparkSession.close();
	}

}
