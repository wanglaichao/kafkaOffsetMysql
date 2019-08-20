import java.util.Properties

import com.fanli.bigdata.multiple.KafkaPropertyBasic
import org.apache.commons.cli.{CommandLine, DefaultParser, Options, PosixParser}

/**
  * Created by laichao.wang on 2018/11/28.
  */
object Commons_cli_test {

  def main(args: Array[String]) {
    val s = "last10m"
    val minPattern = "last([\\d]+)m".r
    s match {
      case minPattern(m)=>println(m)
      case _ =>println("nothing")

    }

  }

  def main1(args: Array[String]) {
     val options: Options = new Options
     options.addOption("f",true,"")
     val parser: DefaultParser = new DefaultParser
     val parse: CommandLine = parser.parse(options,args)

    if ( parse.hasOption("f")){
       val value: String = parse.getOptionValue("f")
       println(value)
    }
    val optionValue: String = parse.getOptionValue("h")

    println(1)

  }


}
