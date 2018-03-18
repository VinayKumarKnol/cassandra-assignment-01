package edu.knoldus.model

import org.apache.log4j.Logger

object Application extends App {

  val log = Logger.getLogger(this.getClass)
  val queriesResult = new Queries
  log.info(s"Creating Key space \n")
  log.info(s"${queriesResult.createKeyspaceForAssignment}\n")
  log.info(s"${queriesResult.createTableEmployee}\n")
  log.info(s"${queriesResult.insertDataToEmployee()}\n")
  log.info(s"${queriesResult.createTableByCity}\n")
  log.info(s"${queriesResult.insertDataToEmployeeCity()}\n")
  log.info(s"${queriesResult.createIndexOnCity}\n")
  log.info(s"Data fetched by Id            : \n ${queriesResult.fetchDataOfAnId}\n")
  log.info(s"Data after update             : \n ${queriesResult.updateRecord}\n")
  log.info(s"Data fetched by salary and id : \n ${queriesResult.fetchDataBySalaryAndId}\n")
  log.info(s"Data fetched by city          : \n ${queriesResult.fetchDataByCity}\n")
  log.info(s"Data post deletion            : \n ${queriesResult.deleteDataByCity}\n")


}
