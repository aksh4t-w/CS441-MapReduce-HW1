package CS441HW1

import com.typesafe.config.ConfigFactory
import CS441HW1.HelperUtils.CreateLogger
import CS441HW1.MR_Task1.logger
import CS441HW1.MR_Task2.logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
 * This map reduce job takes in log files in a folder as input and outputs the number of messages of each log type
 * (INFO, DEBUG, WARN, ERROR) in all the generated logs.
 */

object MR_Task3 {
  val logger = CreateLogger(this.getClass)
  private final val one = new IntWritable(1)

  class TokenMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Logic for mapper

      val s: String = value.toString
      val t: Array[String] = s.split(" +")

      // This part checks whether the log has one of the given types or not. If yes, it writes to the context.
      if (t.length > 2 && (t(2) == "INFO" || t(2) == "ERROR" || t(2) == "WARN" || t(2) == "DEBUG")) {
        context.write(new Text(t(2)), one)
      }
    }
  }

  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // Logic for reducer
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(new Text(key.toString), new IntWritable(sum))
    }
  }
  
  @main def runTask3(inputPath: String, outputPath: String): Unit = {
    val configfile = ConfigFactory.load()
    val pattern = configfile.getString("MR_Tasks_Conf.Pattern")
    val time_interval = configfile.getString("MR_Tasks_Conf.TimeInterval")

    logger.info("Setting configuration values")
    val conf = new Configuration()
    conf.set("Interval", time_interval)
    conf.set("Pattern", pattern)
    conf.set("mapred.textoutputformat.separator", ",")
    conf.set("mapreduce.job.reduces", "1")
    
    logger.info("Setting up and starting map-reduce job")
    val job1 = Job.getInstance(conf)
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[TokenMapper])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[IntWritable])
    job1.setReducerClass(classOf[LogReducer])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(inputPath))
    FileOutputFormat.setOutputPath(job1, new Path(outputPath))

    System.exit(if (job1.waitForCompletion(true)) 0 else 1)
  }
}
