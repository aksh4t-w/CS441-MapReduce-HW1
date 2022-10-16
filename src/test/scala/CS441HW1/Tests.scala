package CS441HW1

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}
import java.util.regex.Pattern

class Tests extends AnyFlatSpec with Matchers {
  val configfile: Config = ConfigFactory.load()
  val Log: Logger = LoggerFactory.getLogger(getClass)

  val pattern = configfile.getString("MR_Tasks_Conf.Pattern")
  val test_string1 = "14:18:43.447 [scala-execution-context-global-149] WARN  HelperUtils.Parameters$ - BFHGTjzJL:^jN5qG6mC7mK6tae0~uGmrD+n+F|W"
  val test_string2 = "14:19:01.711 [scala-execution-context-global-149] INFO  HelperUtils.Parameters$ - .NzRJ4vdWPrn\\Ye{fjw!|\\"
  val time_interval = configfile.getInt("MR_Tasks_Conf.TimeInterval")

  behavior of "Configuration specified in the config file"
  it should "The pattern specified in the configuration file" in {
    pattern.isInstanceOf[String] should be (true)
  }

  it should "Match the pattern specified in the configuration file with test string 1" in {
    val compiled_pattern = Pattern.compile(pattern)
    val matcher = compiled_pattern.matcher(test_string1)
    matcher.find() should be (true)
  }

  it should "Match the pattern specified in the configuration file with test string 2" in {
    val compiled_pattern = Pattern.compile(pattern)
    val matcher = compiled_pattern.matcher(test_string2)
    matcher.find() should be(false)
  }

  it should "Type of time interval in the config file" in {
    time_interval.isInstanceOf[Int] should be (true)
  }

  it should "Time interval in the config file" in {
    time_interval should be > 5
  }

  it should "Check length of test pattern" in {
    pattern.length should be > 10
  }
}
