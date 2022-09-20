package local.tchile.bigdata.migracion_siscli

import org.apache.hadoop.hdfs.tools.HDFSConcat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object proceso {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // BEGIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // ---------------------------------------------------------------

    // PARAMETROS

    val CUSTOMER_CONTACT_REL_CURRENT_IND = 1
    val CUSTOMER_CONTACT_REL_CONTACT_ROLE_KEY = 1562
    val BLNG_ARNG_CTCT_REL_CURRENT_FLAG = 1
    val BLNG_ARNG_CTCT_REL_CONTACT_ROLE_KEY = 551
    val CUSTOMER_CONTRACT_RUT_KEY = 1005017244
    val SUBSCRIBER_CONTACT_REL_CURRENT_IND = 1
    val SUBSCRIBER_CONTACT_REL_CONTACT_ROLE_KEY = 548

    // ---------------------------------------------------------------

    // PATHS

    val hdfsPath = "/data/parque_asignado/parque/b2b/scel/migracionSISCLI/"
    val basePath = "/modelos/ods/"
    val customerPath = s"${basePath}customer/conformado/"
    val customerContactRelPath = s"${basePath}customer_contact_rel/conformado/"
    val contactAddressRelPath = s"${basePath}contact_address_rel/conformado/"
    val contactPath = s"${basePath}contact/conformado/"
    val financialAccountPath = s"${basePath}financial_account/conformado/"
    val billingArrangementPath = s"${basePath}billing_arrangement/conformado/"
    val blngArngCtctRelPath = s"${basePath}blng_arng_ctct_rel/conformado/"
    val addressPath = s"${basePath}address/conformado/"
    val financialAccountStatusPath = s"${basePath}financial_account_status/conformado/"
    val subscribersPath = s"${basePath}subscribers/conformado/"
    val contractRutPath = s"${basePath}contract_rut/conformado/"
    val subscriberContactRelPath = s"${basePath}subscriber_contact_rel/conformado/"

    // ---------------------------------------------------------------
    // END PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // BEGIN PROCESO
    // ---------------------------------------------------------------

    val customer = spark.read.parquet(customerPath).
      filter($"CONTRACT_RUT_KEY" === CUSTOMER_CONTRACT_RUT_KEY).
      select("CUSTOMER_KEY", "CONTRACT_RUT_KEY", "RUT_ID", "CUSTOMER_LEGAL_NAME", "GIRO", "DML_IND")
    val customerContactRel = spark.read.parquet(customerContactRelPath).
      //filter($"CURRENT_IND" === CUSTOMER_CONTACT_REL_CURRENT_IND and $"CONTACT_ROLE_KEY" === CUSTOMER_CONTACT_REL_CONTACT_ROLE_KEY and $"DML_IND" =!= "DELETE").
      select("CUSTOMER_KEY", "CONTACT_KEY", "CURRENT_IND", "CONTACT_ROLE_KEY", "DML_IND")
    val contactAddressRel = spark.read.parquet(contactAddressRelPath).
      //filter($"ROLE_NAME" === "Representante legal" and $"DML_IND" =!= "DELETE").
      select("CONTACT_KEY", "ROLE_NAME", "DML_IND")
    val contact = spark.read.parquet(contactPath).
      //filter($"DML_IND" =!= "DELETE").
      select("CONTACT_KEY", "IDENTIFICATION_DOCUMENT_1_NUMB", "FIRST_NAME", "LAST_NAME", "EMAIL_ID", "DML_IND")
    val financialAccount = spark.read.parquet(financialAccountPath).
      select("FINANCIAL_ACCOUNT_KEY", "FINANCIAL_ACCOUNT_STATUS_KEY", "CUSTOMER_KEY")
    val billingArrangement = spark.read.parquet(billingArrangementPath).
      select("FINANCIAL_ACCOUNT_KEY", "BILLING_ARRANGEMENT_KEY")
    val blngArngCtctRel = spark.read.parquet(blngArngCtctRelPath).
      //filter($"CURRENT_FLAG" === BLNG_ARNG_CTCT_REL_CURRENT_FLAG and $"CONTACT_ROLE_KEY" === BLNG_ARNG_CTCT_REL_CONTACT_ROLE_KEY).
      select("BILLING_ARRANGEMENT_KEY", "ADDRESS_KEY", "CURRENT_FLAG", "CONTACT_ROLE_KEY")
    val address = spark.read.parquet(addressPath).
      select("ADDRESS_KEY", "ADDRESS_LINE_1")
    val financialAccountStatus = spark.read.parquet(financialAccountStatusPath).
      select("FINANCIAL_ACCOUNT_STATUS_KEY", "FINANCIAL_ACCOUNT_STATUS_DESC")
    val subscribers = spark.read.parquet(subscribersPath).
      select("CUSTOMER_KEY", "SUBSCRIBER_KEY", "PRIMARY_RESOURCE_VALUE", "PRIMARY_RESOURCE_TYPE_KEY")
    val contractRut = spark.read.parquet(contractRutPath).
      select("CONTRACT_RUT_KEY", "CONTRACT_RUT_DESC")
    val subscriberContactRel = spark.read.parquet(subscriberContactRelPath).
      //filter($"CURRENT_IND" === SUBSCRIBER_CONTACT_REL_CURRENT_IND and $"CONTACT_ROLE_KEY" === SUBSCRIBER_CONTACT_REL_CONTACT_ROLE_KEY).
      select("SUBSCRIBER_KEY", "ADDRESS_KEY", "CURRENT_IND", "CONTACT_ROLE_KEY")
    val address2 = address

    val tempPart1 = customer.
      join(customerContactRel,
        customer("CUSTOMER_KEY") === customerContactRel("CUSTOMER_KEY")
        and customerContactRel("CURRENT_IND") === CUSTOMER_CONTACT_REL_CURRENT_IND
        and customerContactRel("CONTACT_ROLE_KEY") === CUSTOMER_CONTACT_REL_CONTACT_ROLE_KEY
        and customerContactRel("DML_IND") =!= "DELETE",
        "left"
      ).
      join(contactAddressRel,
        customerContactRel("CONTACT_KEY") === contactAddressRel("CONTACT_KEY")
        and contactAddressRel("DML_IND") =!= "DELETE",
        "left"
      ).
      join(contact,
        contactAddressRel("CONTACT_KEY") === contact("CONTACT_KEY")
        and contact("DML_IND") =!= "DELETE",
        "left"
      ).
      join(financialAccount, customer("CUSTOMER_KEY") === financialAccount("CUSTOMER_KEY"), "left").
      join(billingArrangement, financialAccount("FINANCIAL_ACCOUNT_KEY") === billingArrangement("FINANCIAL_ACCOUNT_KEY"), "left").
      join(blngArngCtctRel,
        billingArrangement("BILLING_ARRANGEMENT_KEY") === blngArngCtctRel("BILLING_ARRANGEMENT_KEY")
        and blngArngCtctRel("CURRENT_FLAG") === BLNG_ARNG_CTCT_REL_CURRENT_FLAG
        and blngArngCtctRel("CONTACT_ROLE_KEY") === BLNG_ARNG_CTCT_REL_CONTACT_ROLE_KEY,
        "left"
      ).
      join(address, blngArngCtctRel("ADDRESS_KEY") === address("ADDRESS_KEY"), "left").
      join(financialAccountStatus, financialAccount("FINANCIAL_ACCOUNT_STATUS_KEY") === financialAccountStatus("FINANCIAL_ACCOUNT_STATUS_KEY"), "left").
      join(subscribers, customer("CUSTOMER_KEY") === subscribers("CUSTOMER_KEY"), "left").
      join(contractRut, customer("CONTRACT_RUT_KEY") === contractRut("CONTRACT_RUT_KEY"), "inner").
      select(
        customer("CUSTOMER_KEY"),
        customer("CONTRACT_RUT_KEY"),
        customer("RUT_ID"),
        customer("CUSTOMER_LEGAL_NAME"),
        customer("GIRO"),
        customer("DML_IND"),
        customerContactRel("CONTACT_KEY"),
        contactAddressRel("ROLE_NAME"),
        contact("FIRST_NAME"),
        contact("LAST_NAME"),
        contact("EMAIL_ID"),
        contact("IDENTIFICATION_DOCUMENT_1_NUMB"),
        financialAccount("FINANCIAL_ACCOUNT_KEY"),
        financialAccount("FINANCIAL_ACCOUNT_STATUS_KEY"),
        billingArrangement("BILLING_ARRANGEMENT_KEY"),
        blngArngCtctRel("ADDRESS_KEY"),
        address("ADDRESS_LINE_1"),
        financialAccountStatus("FINANCIAL_ACCOUNT_STATUS_DESC"),
        subscribers("PRIMARY_RESOURCE_VALUE"),
        subscribers("SUBSCRIBER_KEY"),
        subscribers("PRIMARY_RESOURCE_TYPE_KEY"),
        contractRut("CONTRACT_RUT_DESC")
      ).
      filter(customer("DML_IND") =!= "DELETE" and contactAddressRel("ROLE_NAME") === "Representante legal")

    tempPart1.write.mode("overwrite").parquet(s"${hdfsPath}temp/part1")

    val part1Hdfs = spark.read.parquet(s"${hdfsPath}temp/part1")

    val tempPart2 = part1Hdfs.
      join(subscriberContactRel,
        part1Hdfs("SUBSCRIBER_KEY") === subscriberContactRel("SUBSCRIBER_KEY")
        and subscriberContactRel("CURRENT_IND") === SUBSCRIBER_CONTACT_REL_CURRENT_IND
        and subscriberContactRel("CONTACT_ROLE_KEY") === SUBSCRIBER_CONTACT_REL_CONTACT_ROLE_KEY,
        "left"
      ).
      join(address2, subscriberContactRel("ADDRESS_KEY") === address2("ADDRESS_KEY"), "left").
      withColumn("cuentaFinanciera", part1Hdfs("FINANCIAL_ACCOUNT_KEY")).
      withColumn("acuerdoFacturacion", part1Hdfs("BILLING_ARRANGEMENT_KEY")).
      withColumn("contactoPersona", concat(part1Hdfs("FIRST_NAME"), lit("  "), part1Hdfs("LAST_NAME"))).
      withColumn("correoContacto", part1Hdfs("EMAIL_ID")).
      withColumn("direccionFacturacion", part1Hdfs("ADDRESS_LINE_1")).
      withColumn("direccionInstalacion", address2("ADDRESS_LINE_1")).
      withColumn("estadoCuenta", part1Hdfs("FINANCIAL_ACCOUNT_STATUS_DESC")).
      withColumn("giroDescripcion", part1Hdfs("GIRO")).
      withColumn("msidn_ó_SERVICE_ID", part1Hdfs("PRIMARY_RESOURCE_VALUE")).
      withColumn("razonSocial", part1Hdfs("CUSTOMER_LEGAL_NAME")).
      withColumn("RUT_CLIENTE", part1Hdfs("RUT_ID")).
      withColumn("rutCliente", part1Hdfs("CUSTOMER_KEY")).
      withColumn("TIPO_CLIENTE", part1Hdfs("CONTRACT_RUT_DESC")).
      withColumn("CONTACTO", part1Hdfs("CONTACT_KEY")).
      withColumn("CONTACT_ROLE_NAME", part1Hdfs("ROLE_NAME")).
      withColumn("CONTACT_RUT_ID", part1Hdfs("IDENTIFICATION_DOCUMENT_1_NUMB")).
      groupBy(
        "cuentaFinanciera",
        "acuerdoFacturacion",
        "contactoPersona",
        "correoContacto",
        "direccionFacturacion",
        "direccionInstalacion",
        "estadoCuenta",
        "giroDescripcion",
        "msidn_ó_SERVICE_ID",
        "razonSocial",
        "RUT_CLIENTE",
        "rutCliente",
        "TIPO_CLIENTE",
        "PRIMARY_RESOURCE_TYPE_KEY",
        "CONTACTO",
        "CONTACT_ROLE_NAME",
        "CONTACT_RUT_ID"
      ).
      count()

    tempPart2.write.mode("overwrite").parquet(s"${hdfsPath}temp/part2")

    spark.read.parquet(s"${hdfsPath}temp/part2").drop("count").repartition(1).write.mode("overwrite").format("parquet").option("path",s"${hdfsPath}conformado/").saveAsTable("stg_datalakeb2b.fil_siscli")

//    conformado.repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", ";").csv(s"${hdfsPath}csv/test1.csv")
//
//    conformado.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").save(s"${hdfsPath}csv/")
//
//    conformado.coalesce(1).write.mode("overwrite").option("header", "true").csv(s"${hdfsPath}csv/test1.csv")
//
//    conformado.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ";").save(s"${hdfsPath}csv/test1.csv")

    // ---------------------------------------------------------------
    // END PROCESO
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // BEGIN ESCRITURA HDFS
    // ---------------------------------------------------------------



    // ---------------------------------------------------------------
    // END ESCRITURA HDFS
    // ---------------------------------------------------------------

  }
}
