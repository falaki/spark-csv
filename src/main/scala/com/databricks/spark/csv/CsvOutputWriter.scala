package com.databricks.spark.csv

import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext, TaskAttemptID}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.OutputWriter

import org.apache.spark.sql.Row
import org.apache.spark.deploy.SparkHadoopUtil

import com.databricks.spark.csv.util.Parameters

private[csv] class CsvOutputWriter(
    path: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    parameters: Map[String, String]) extends OutputWriter {

  private val params = new Parameters(parameters)
  private val delimiter = params.getChar("delimiter", Some(',')).get
  private val escape = params.getChar("escape", Some('\0')).get
  private val quote = params.getChar("quote", Some('\"')).get
  private val nullValue = parameters.getOrElse("nullValue", "null")
  private val generateHeader = params.getBool("header", default = true)
  private val codec = parameters.get("codec")

  // create the Generator without separator inserted between 2 records
  private val text = new Text()

  private val recordWriter: RecordWriter[NullWritable, Text] = {
    new TextOutputFormat[NullWritable, Text]() {
      override def getDefaultWorkFile(context: TaskAttemptContext, extension: String): Path = {
        val configuration = SparkHadoopUtil.get.getConfigurationFromJobContext(context)
        val uniqueWriteJobId = configuration.get("spark.sql.sources.writeJobUUID")

        val taskAttemptId: TaskAttemptID = {
          // Use reflection to get the TaskAttemptID. This is necessary because TaskAttemptContext
          // is a class in Hadoop 1.x and an interface in Hadoop 2.x.
          val method = context.getClass.getMethod("getTaskAttemptID")
          method.invoke(context).asInstanceOf[TaskAttemptID]
        }
        val split = taskAttemptId.getTaskID.getId
        new Path(path, f"part-r-$split%05d-$uniqueWriteJobId$extension")
      }
    }.getRecordWriter(context)
  }

  private var firstRow: Boolean = generateHeader

  private val csvFormat = defaultCsvFormat
    .withDelimiter(delimiter)
    .withQuote(quote)
    .withEscape(escape)
    .withSkipHeaderRecord(false)
    .withNullString(nullValue)

  private val header = if (generateHeader) {
    csvFormat.format(dataSchema.fieldNames.map(_.asInstanceOf[AnyRef]): _*)
  } else {
    "" // There is no need to generate header in this case
  }


  override def write(row: Row): Unit = {
    val rowString = csvFormat.format(row.toSeq.map(_.asInstanceOf[AnyRef]): _*)
    val outputString = if (firstRow) {
      firstRow = false
      header + csvFormat.getRecordSeparator + rowString
    } else {
      rowString
    }

    text.clear()
    text.set(outputString.toString)
    recordWriter.write(NullWritable.get(), text)
  }

  override def close(): Unit = {
    recordWriter.close(context)
  }

}

