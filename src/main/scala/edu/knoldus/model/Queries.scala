package edu.knoldus.model

import com.datastax.driver.core.{ConsistencyLevel, Row, Session}

import scala.collection.JavaConverters._

class Queries {

  final val SELECT_QUERY: String = "SELECT * FROM employee"

  val cassandraSession: Session = ConfigCassandra.getCassandraSession
  cassandraSession.getCluster.getConfiguration.getQueryOptions
    .setConsistencyLevel(ConsistencyLevel.QUORUM)

  def createKeyspaceForAssignment: String = {
    cassandraSession.execute(s"CREATE KEYSPACE IF NOT EXISTS ${ConfigCassandra.keyspace} " +
      s"WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }")
    cassandraSession.execute(s"USE ${ConfigCassandra.keyspace} ")
    s"Keyspace has been created!"
  }

  def createTableEmployee: String = {
    val createTableQuery: String = "CREATE TABLE IF NOT EXISTS employee" +
      "(emp_id int, emp_name text,emp_city text,emp_salary varint" +
      ",emp_phone varint,PRIMARY KEY(emp_id,emp_salary) )"
    cassandraSession.execute(createTableQuery)
    s"Employee table has been created successfully"
  }

  def createTableByCity: String = {
    val createTableQuery = "CREATE TABLE IF NOT EXISTS EmployeeByCity" +
      "(emp_id int, emp_name text,emp_city text PRIMARY KEY,emp_salary varint" +
      ",emp_phone varint)"
    cassandraSession.execute(createTableQuery)
    s"EmployeeByCity table created"
  }

  def insertDataToEmployee(): String = {
    val vinayDetail = "INSERT INTO employee" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(1,'Vinay','Delhi',50000,9905432112)" +
      " IF NOT EXISTS "
    val shahabDetail = "INSERT INTO employee" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(2,'Shahab','Delhi',50000,9905432113)" +
      " IF NOT EXISTS "
    val sudeepDetail = "INSERT INTO employee" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(3,'Sudeep','Jharkhand',80000,9905432114)" +
      " IF NOT EXISTS "
    val sauravDetail = "INSERT INTO employee" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(4,'saurav','Delhi',10000,9905432115)" +
      " IF NOT EXISTS "

    cassandraSession.execute(vinayDetail)
    cassandraSession.execute(shahabDetail)
    cassandraSession.execute(sudeepDetail)
    cassandraSession.execute(sauravDetail)
    s"Data inserted in Employee table by city"

  }

  def insertDataToEmployeeCity(): String = {
    val vinayDetail = "INSERT INTO EmployeeByCity" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(1,'Vinay','Delhi',50000,9905432112)" +
      " IF NOT EXISTS "
    val shahabDetail = "INSERT INTO EmployeeByCity" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(2,'Shahab','Delhi',50000,9905432113)" +
      " IF NOT EXISTS "
    val sudeepDetail = "INSERT INTO EmployeeByCity" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(3,'Sudeep','Jharkhand',80000,9905432114)" +
      " IF NOT EXISTS "
    val sauravDetail = "INSERT INTO EmployeeByCity" +
      "(emp_id, emp_name,emp_city ,emp_salary ,emp_phone)" +
      " VALUES(4,'saurav','Delhi',10000,9905432115)" +
      " IF NOT EXISTS "

    cassandraSession.execute(vinayDetail)
    cassandraSession.execute(shahabDetail)
    cassandraSession.execute(sudeepDetail)
    cassandraSession.execute(sauravDetail)
    s"Data inserted in Employee table by city"

  }

  def createIndexOnCity: String = {
    val createCityIndex = "CREATE INDEX IF NOT EXISTS  employeecityindex " +
      "ON employee(emp_city)"
    cassandraSession.execute(createCityIndex)
    s"Index on city has be created!"
  }

  def fetchDataOfAnId: List[Row] = {
    val fetchDataQuery = "SELECT * FROM employee WHERE emp_id = 3"
    cassandraSession.execute(fetchDataQuery).asScala.toList
  }

  def updateRecord: List[Row] = {
    val updateRecordQuery = "UPDATE employee SET emp_city = 'chandigarh'" +
      " WHERE emp_id = 1 AND emp_salary = 50000 "
    cassandraSession.execute(updateRecordQuery)
    cassandraSession.execute(SELECT_QUERY).asScala.toList
  }

  def fetchDataBySalaryAndId: List[Row] = {
    val fetchDataQuery = "SELECT * FROM employee " +
      "WHERE emp_salary > 30000 AND emp_id = 1"
    cassandraSession.execute(fetchDataQuery).asScala.toList
  }

  def fetchDataByCity: List[Row] = {
    val fetchByCityQuery = "SELECT * FROM employee WHERE emp_city = 'chandigarh'"
    cassandraSession.execute(fetchByCityQuery).asScala.toList
  }

  def deleteDataByCity: List[Row] = {
    val deleteData = "DELETE FROM EmployeeByCity where emp_city = 'chandigarh'"
    cassandraSession.execute(deleteData)
    cassandraSession.execute(SELECT_QUERY).asScala.toList
  }

}
