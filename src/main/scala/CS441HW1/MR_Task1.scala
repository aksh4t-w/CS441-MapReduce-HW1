package CS441HW1

import com.typesafe.config.ConfigFactory
import CS441HW1.HelperUtils.CreateLogger
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
 * (INFO, DEBUG, WARN, ERROR) divided across time intervals of n seconds where n is passed as a parameter while
 * running the program.
 */

//class MR_Task1 {
//
//}

object MR_Task1:
  val logger = CreateLogger(this.getClass)
  private final val one = new IntWritable(1)

  // This class defines the mapper for collating the key and value pairs from the logs based on the time intervals.
  class TokenMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
  //  Interval length is extracted from the config file
      val interval_length = context.getConfiguration.get("Interval").toInt

      val s: String = value.toString
      val t: Array[String] = s.split(" +")

  //  Check whether the log type is one of the 4 given types
      if (t.length > 2 && (t(2) == "INFO" || t(2) == "ERROR" || t(2) == "WARN" || t(2) == "DEBUG")) {
        val time = new SimpleDateFormat("HH:mm:ss.SSS").parse(t(0))

        val time_group = time.getTime.toInt / (interval_length * 1000)
        val group_type_key = time_group.toString + ' ' + t(2)
        context.write(new Text(group_type_key), one)
      }
    }
  }

  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      val interval_length = context.getConfiguration.get("Interval").toInt
      val key_split = key.toString.split(' ')
      val time_convert = key_split(0).toInt * (interval_length * 1000)
      val time_group = new SimpleDateFormat("HH:mm:ss").format(time_convert)
      context.write(new Text(time_group + ',' + key_split(1)), new IntWritable(sum))
    }
  }

  @main def runTask1(inputPath: String, outputPath: String): Unit =
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
