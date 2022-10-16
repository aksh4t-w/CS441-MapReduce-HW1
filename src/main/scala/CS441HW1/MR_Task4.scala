package CS441HW1

import com.typesafe.config.ConfigFactory
import CS441HW1.HelperUtils.CreateLogger
import CS441HW1.MR_Task1.logger
import CS441HW1.MR_Task3.logger
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
 * This map reduce job takes in log files in a folder as input, finds the logs with the matching regex pattern given
 * and outputs the maximum length of the pattern for all four log types. The mapper maps matches the pattern and sends 
 * the count of each pattern to the reducer which later finds the maximum out of those lengths and writes it in the output
 * for corresponding log types.
 */


object MR_Task4 {
  val logger = CreateLogger(this.getClass)

  class TokenMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Logic for mapper
      val match_pattern = Pattern.compile(context.getConfiguration.get("Pattern"))

      val s: String = value.toString
      val t: Array[String] = s.split(" +")
      val compiled_pattern = match_pattern.matcher(t(t.length - 1))

      // check for correct log then writing the pattern length to the context if match is found.
      if (t.length > 2 && (t(2) == "INFO" || t(2) == "ERROR" || t(2) == "WARN" || t(2) == "DEBUG")) {
        if (compiled_pattern.find()) {
          val len = t(t.length - 1).length
          context.write(new Text(t(2)), new IntWritable(len))
        }
      }
    }
  }

  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      // Logic for reducer
      val maxLength = values.asScala.max
      context.write(new Text(key.toString), maxLength)
    }
  }

  @main def runTask4(inputPath: String, outputPath: String): Unit = {
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
