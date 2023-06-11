package com.oneflow.de

import com.amazonaws.services.glue.{GlueContext, JsonOptions}
import org.apache.spark.sql.SparkSession

class AwsGlueApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("OneFlow Glue ETL")
      .getOrCreate()
    val sc = spark.sparkContext
    val glueContext = new GlueContext(sc)

    // Read data from MySQL
    val df_account = glueContext.getCatalogSource(
      database = "mysql_db",
      tableName = "account",
      redshiftTmpDir = "",
      transformationContext = "mysql_df"
    ).getDynamicFrame()
      .withColumnRenamed("id", "account_id")

    val df_user = glueContext.getCatalogSource(
      database = "mysql_db",
      tableName = "user",
      redshiftTmpDir = "",
      transformationContext = "mysql_df"
    ).getDynamicFrame()
      .withColumnRenamed("id", "user_id")

    // Join data
    val df_join = df_account.join(
      df_user,
      Seq("account_id"),
      "inner"
    )

    //Filter data
    val df_filter = df_join.filter("is_demo = False")

    // Select columns
    val df_result = df_filter.selectFields(Seq(
      "user_id",
      "email",
      "firstname",
      "lastname",
      "language",
      "is_stakeholder",
      "last_visit_time",
      "account_id",
      "plan",
      "license_count",
      "purchase_time",
      "customer_io_opted_in"
    ))


    // Write data to Redshift
    glueContext.getSinkWithFormat(
      connectionType = "redshift",
      options = JsonOptions(Map(
        "dbtable" -> "customer_io_user",
        "database" -> "redshift_db",
        "aws_iam_role" -> "",
        "aws_region" -> "eu-west-1"
      )),
      transformationContext = "redshift_df"
    ).writeDynamicFrame(df_result)
  }
}
