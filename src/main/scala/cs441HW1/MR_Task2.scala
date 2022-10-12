package cs441HW1

import com.typesafe.config.ConfigFactory
import cs441HW1.HelperUtils.CreateLogger
import cs441HW1.MR_Task1.logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import java.lang
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import scala.jdk.CollectionConverters.IterableHasAsScala
/**
 * This map reduce job takes in log files in a folder as input and outputs the time intervals sorted
 * in descending order that contained most log messages of the type ERROR with injected regex pattern string instances.
 * It makes use of two map reduce operations and one comparator operator for sorting the result based on the keys which
 * are the count of specified logs given.
 */

//class MR_Task2 {
//
//}

object MR_Task2 {
  val logger = CreateLogger(this.getClass)
  private final val one = new IntWritable(1)

  class TokenMapper extends Mapper[Object, Text, Text, IntWritable] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      // Logic for mapper
      val interval_length = context.getConfiguration.get("Interval").toInt
      val match_pattern = Pattern.compile(context.getConfiguration.get("Pattern"))

      val s: String = value.toString
      val t: Array[String] = s.split(" +")
      val compiled_pattern = match_pattern.matcher(t(t.length - 1))

      if (t.length > 2 && t(2) == "ERROR") {
        if (compiled_pattern.find()) {
          val time = new SimpleDateFormat("HH:mm:ss.SSS").parse(t(0))
          val time_group = time.getTime.toInt / (interval_length * 1000)
          context.write(new Text(time_group.toString), one)
        }
      }
    }
  }

  class LogReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(new Text(key.toString()), new IntWritable(sum))
    }
  }

  class Comparator extends WritableComparator(classOf[IntWritable], true) {
    @SuppressWarnings(Array("rawtypes"))
    override def compare(w1: WritableComparable[_], w2: WritableComparable[_]): Int = {
      val key1 = w1.asInstanceOf[IntWritable]
      val key2 = w2.asInstanceOf[IntWritable]
      -1 * key1.compareTo(key2)
    }
  }

  class FinalMapper extends Mapper[Object, Text, IntWritable, Text] {
    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {
      val pattern = Pattern.compile("(\\d+)\\s+(\\d+)")
      val matcher = pattern.matcher(value.toString)
      if (matcher.find()) {
        val interval_length = context.getConfiguration.get("Interval").toInt
        val time_convert = matcher.group(2).toInt * (interval_length * 1000)
        val time_group = new SimpleDateFormat("HH:mm:ss").format(time_convert)
        context.write(new IntWritable(matcher.group(2).toInt), new Text(time_group))
      }
    }
  }

  class FinalReducer extends Reducer[Object, Text, IntWritable, Text] {
    override def reduce(key: Object, values: lang.Iterable[Text], context: Reducer[Object, Text, IntWritable, Text]#Context): Unit = {
      values.forEach(time => {
        context.write(new IntWritable(key.toString.toInt), new Text(time.toString))
      })
    }
  }

  @main def runTask2(inputPath: String, outputPath1: String, outputPath2: String): Unit = {
    val configfile = ConfigFactory.load()
    val pattern = configfile.getString("MR_Tasks_Conf.Pattern")
    val time_interval = configfile.getString("MR_Tasks_Conf.TimeInterval")

    logger.info("Setting configuration values")
    val conf = new Configuration()
    conf.set("Interval", time_interval)
    conf.set("Pattern", pattern)

    logger.info("Setting up and starting map-reduce job1")
    val job1 = Job.getInstance(conf, "Parse Messages")
    job1.setJarByClass(this.getClass)
    job1.setMapperClass(classOf[TokenMapper])
    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[IntWritable])
    job1.setReducerClass(classOf[LogReducer])
    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job1, new Path(inputPath))
    FileOutputFormat.setOutputPath(job1, new Path(outputPath1))

    if (job1.waitForCompletion(true)) {
      logger.info("Setting up and starting map-reduce job2")
      conf.set("mapred.textoutputformat.separator", ",")
      val job2 = Job.getInstance(conf, "Job2")
      job2.setSortComparatorClass(classOf[Comparator])

      job2.setMapperClass(classOf[FinalMapper])
      job2.setMapOutputKeyClass(classOf[IntWritable])
      job2.setMapOutputValueClass(classOf[Text])

      job2.setReducerClass(classOf[FinalReducer])
      job2.setOutputKeyClass(classOf[IntWritable])
      job2.setOutputValueClass(classOf[Text])

      FileInputFormat.addInputPath(job2, new Path(outputPath1))
      FileOutputFormat.setOutputPath(job2, new Path(outputPath2))

      System.exit(if (job2.waitForCompletion(true)) 0 else 1)
    }
  }
}
