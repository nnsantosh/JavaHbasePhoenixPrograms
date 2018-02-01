This project has java  code to add rows and columns, delete a column from a row or multiple rows and to retrieve column values from row or multiple rows of a Hbase database.
1. To set up hbase locally download the latest version of hbase from https://hbase.apache.org
2. Refer https://hbase.apache.org/book.html#quickstart for standalone set up
3. Once you have the hbase up and running in local machine then you can run the java classes present in this project.
To run phoenix related programs:
* Download and expand the latest phoenix-[version]-bin.tar.
* Add the phoenix-[version]-server.jar to the classpath of all HBase region server and master and remove any previous version. An easy way to do this is to copy it into the HBase lib directory (use phoenix-core-[version].jar for Phoenix 3.x)
* Restart HBase.
* Add the phoenix-[version]-client.jar to the classpath of any Phoenix client.