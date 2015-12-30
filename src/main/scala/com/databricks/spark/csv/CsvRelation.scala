/*
 * Copyright 2014 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.csv

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

import com.google.common.base.Objects
import org.apache.commons.csv._
import org.apache.hadoop.fs.FileStatus
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.hadoop.mapreduce.{TaskAttemptContext, Job}
import org.apache.spark.sql.types._

import com.databricks.spark.csv.readers.{BulkCsvReader, LineCsvReader}
import com.databricks.spark.csv.util._

private[csv] case class CsvRelation(
    private val inputRDD: Option[RDD[String]],
    override val paths: Array[String],
    private val maybeDataSchema: Option[StructType],
    override val userDefinedPartitionColumns: Option[StructType],
    private val parameters: Map[String, String])
    (@transient val sqlContext: SQLContext) extends HadoopFsRelation {


  override def dataSchema: StructType = maybeDataSchema match {
    case Some(structType) => structType
    case None => inferSchema(paths)
  }

  @transient
  private val params = new Parameters(parameters)

  private val delimiter = TypeCast.toChar(parameters.getOrElse("delimiter", ","))
  private val parserLib = parameters.getOrElse("parserLib", ParserLibs.DEFAULT)
  private val parseMode = parameters.getOrElse("mode", "PERMISSIVE")
  private val charset = parameters.getOrElse("charset", TextFile.DEFAULT_CHARSET.name())
  // TODO validate charset?
  val codec = parameters.getOrElse("codec", null)

  private val quote = params.getChar("quote", Some('\"'))
  private val escape = params.getChar("escape", Some('\\'))
  private val comment = params.getChar("comment", None)

  private val headerFlag = params.getBool("header")
  private val inferSchemaFlag = params.getBool("inferSchema")
  private val treatEmptyValuesAsNullsFlag = params.getBool("treatEmptyValuesAsNulls")
  private val ignoreLeadingWhiteSpaceFlag = params.getBool("ignoreLeadingWhiteSpace")
  private val ignoreTrailingWhiteSpaceFlag = params.getBool("ignoreTrailingWhiteSpace")

  // Limit the number of lines we'll search for a header row that isn't comment-prefixed
  private val MAX_COMMENT_LINES_IN_HEADER = 10

  private val logger = LoggerFactory.getLogger(CsvRelation.getClass)

  // Parse mode flags
  if (!ParseModes.isValidMode(parseMode)) {
    logger.warn(s"$parseMode is not a valid parse mode. Using ${ParseModes.DEFAULT}.")
  }

  if ((ignoreLeadingWhiteSpaceFlag || ignoreTrailingWhiteSpaceFlag) &&
    ParserLibs.isCommonsLib(parserLib)) {
    logger.warn(s"Ignore white space options may not work with Commons parserLib option")
  }

  private val failFast = ParseModes.isFailFastMode(parseMode)
  private val dropMalformed = ParseModes.isDropMalformedMode(parseMode)
  private val permissive = ParseModes.isPermissiveMode(parseMode)

  @transient
  private var cachedRDD: Option[RDD[String]] = None

  private def baseRDD(inputPaths: Array[String]): RDD[String] = {
    inputRDD.getOrElse {
      cachedRDD.getOrElse {
        println("******************")
        println("Reading base RDD from paths: " + inputPaths.mkString(","))
        // println(Thread.currentThread().getStackTrace().toSeq.mkString("\n"))
        val rdd = TextFile.withCharset(sqlContext.sparkContext, inputPaths.mkString(","), charset)
        println(s"Read the RDD with ${rdd.count} rows")
        cachedRDD = Some(rdd)
        rdd
      }
    }
  }

  private def tokenRdd(header: Array[String], inputPaths: Array[String]): RDD[Array[String]] = {
    println("Calling baseRDD function in tokenRdd() function")
    val rdd = baseRDD(inputPaths)
    // Make sure firstLine is materialized before sending to executors
    val firstLine = if (headerFlag) findFirstLine(rdd) else null
    sqlContext.sparkContext.emptyRDD[Array[String]]

    if (ParserLibs.isUnivocityLib(parserLib)) {
      univocityParseCSV(rdd, header, firstLine)
    } else {
      parseCSV(rdd, header, firstLine)
      }
  }

  /**
    * This supports to eliminate unneeded columns before producing an RDD
    * containing all of its tuples as Row objects. This reads all the tokens of each line
    * and then drop unneeded tokens without casting and type-checking by mapping
    * both the indices produced by `requiredColumns` and the ones of tokens.
    */
  override def buildScan(requiredColumns: Array[String], inputs: Array[FileStatus]): RDD[Row] = {
    val schemaFields = schema.fields
    val requiredFields = StructType(requiredColumns.map(schema(_))).fields
    val safeRequiredFields = if (dropMalformed) {
      // If `dropMalformed` is enabled, then it needs to parse all the values
      // so that we can decide which row is malformed.
      requiredFields ++ schemaFields.filterNot(requiredFields.contains(_))
    } else {
      requiredFields
    }
    if (requiredColumns.isEmpty) {
      sqlContext.sparkContext.emptyRDD[Row]
    } else {
      val safeRequiredIndices = new Array[Int](safeRequiredFields.length)
      schemaFields.zipWithIndex.filter {
        case (field, _) => safeRequiredFields.contains(field)
      }.foreach {
        case (field, index) => safeRequiredIndices(safeRequiredFields.indexOf(field)) = index
      }
      val rowArray = new Array[Any](safeRequiredIndices.length)
      val requiredSize = requiredFields.length
      val pathsString = inputs.map(_.getPath.toUri.toString)
      println("////////////////////////////////////////")
      println("Before going into flatMap")
      val header = schemaFields.map(_.name)
      tokenRdd(header, pathsString).flatMap { tokens =>
        println("tokens are: " + tokens)
        if (dropMalformed && schemaFields.length != tokens.size) {
          logger.warn(s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
          None
        } else if (failFast && schemaFields.length != tokens.size) {
          throw new RuntimeException(s"Malformed line in FAILFAST mode: " +
            s"${tokens.mkString(delimiter.toString)}")
        } else {
          val indexSafeTokens = if (permissive && schemaFields.length != tokens.size) {
            tokens ++ new Array[String](schemaFields.length - tokens.size)
          } else {
            tokens
          }
          try {
            var index: Int = 0
            var subIndex: Int = 0
            while (subIndex < safeRequiredIndices.length) {
              index = safeRequiredIndices(subIndex)
              val field = schemaFields(index)
              rowArray(subIndex) = TypeCast.castTo(
                indexSafeTokens(index),
                field.dataType,
                field.nullable,
                treatEmptyValuesAsNullsFlag)
              subIndex = subIndex + 1
            }
            Some(Row.fromSeq(rowArray.take(requiredSize)))
          } catch {
            case nfe: java.lang.NumberFormatException if dropMalformed =>
              logger.warn("Number format exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
            case pe: java.text.ParseException if dropMalformed =>
              logger.warn("Parse Exception. " +
                s"Dropping malformed line: ${tokens.mkString(delimiter.toString)}")
              None
          }
        }
      }
    }
  }

  override def prepareJobForWrite(job: Job): OutputWriterFactory = {
    new OutputWriterFactory {
      override def newInstance(
          path: String,
          dataSchema: StructType,
          context: TaskAttemptContext): OutputWriter =  {
        new CsvOutputWriter(path, dataSchema, context, parameters)
      }
    }
  }

  override def hashCode(): Int = Objects.hashCode(paths.toSet, dataSchema, schema, partitionColumns)

  override def equals(other: Any): Boolean = other match {
    case that: CsvRelation => {
      val equalPath = paths.toSet == that.paths.toSet
      val equalDataSchema = dataSchema == that.dataSchema
      val equalSchema = schema == that.schema
      val equalPartitionColums = partitionColumns == that.partitionColumns

      equalPath && equalDataSchema && equalSchema && equalPartitionColums
    }
    case _ => false
  }

  private def inferSchema(paths: Array[String]): StructType = {
    println("Infer schema is called")
    val rdd = baseRDD(Array(paths.head))
    val firstLine = findFirstLine(rdd)
    val firstRow = if (ParserLibs.isUnivocityLib(parserLib)) {
      new LineCsvReader(
        fieldSep = delimiter,
        quote = quote.getOrElse('\0'),
        escape = escape.getOrElse('\0'),
        commentMarker = comment.getOrElse('\0')).parseLine(firstLine)
    } else {
      val csvFormat = defaultCsvFormat
        .withDelimiter(delimiter)
        .withQuote(quote.get)
        .withEscape(escape.get)
        .withSkipHeaderRecord(false)
      CSVParser.parse(firstLine, csvFormat).getRecords.head.toArray
    }
    val header = if (headerFlag) {
      firstRow
    } else {
      firstRow.zipWithIndex.map { case (value, index) => s"C$index" }
    }

    val parsedRdd = tokenRdd(header, paths)
    if (inferSchemaFlag) {
      InferSchema(parsedRdd, header)
    } else {
      // By default fields are assumed to be StringType
      val schemaFields = header.map { fieldName =>
        StructField(fieldName.toString, StringType, nullable = true)
      }
      StructType(schemaFields)
    }
  }

  /**
    * Returns the first line of the first non-empty file in path
    */
  private def findFirstLine(rdd: RDD[String]) = {
    if (comment.isEmpty) {
      rdd.first()
    } else {
      rdd.take(MAX_COMMENT_LINES_IN_HEADER)
        .find(!_.startsWith(comment.get.toString))
        .getOrElse(sys.error(s"No uncommented header line in " +
          s"first $MAX_COMMENT_LINES_IN_HEADER lines"))
    }
  }

  private def univocityParseCSV(
     file: RDD[String],
     header: Seq[String],
     firstLine: String): RDD[Array[String]] = {
    // If header is set, make sure firstLine is materialized before sending to executors.
    println("Univocity parser is working")
    println("header: " + header)
    println("firstLine: " + firstLine)
    file.mapPartitionsWithIndex({
      case (split, iter) => new BulkCsvReader(
          if (headerFlag) iter.filterNot(_ == firstLine) else iter,
          split,
          headers = header,
          fieldSep = delimiter,
          quote = quote.getOrElse('\0'),
          escape = escape.getOrElse('\0'),
          commentMarker = comment.getOrElse('\0'))
    }, true)
  }

  private def parseCSV(
      file: RDD[String],
      header: Seq[String],
      firstLine: String): RDD[Array[String]] = {

    val csvFormat = defaultCsvFormat
      .withDelimiter(delimiter)
      .withQuote(quote.getOrElse('\0'))
      .withEscape(escape.getOrElse('\0'))
      .withSkipHeaderRecord(false)
      .withHeader(header: _*)
      .withCommentMarker(comment.getOrElse('\0'))

    file.mapPartitions { iter =>
      val filteredIter = if (headerFlag) iter.filterNot(_ == firstLine) else iter
      filteredIter.flatMap { line =>
        try {
          val records = CSVParser.parse(line, csvFormat).getRecords
          if (records.isEmpty) {
            logger.warn(s"Ignoring empty line: $line")
            None
          } else {
            Some(records.head.toArray)
          }
        } catch {
          case NonFatal(e) if !failFast =>
            logger.error(s"Exception while parsing line: $line. ", e)
            None
        }
      }
    }
  }
}
