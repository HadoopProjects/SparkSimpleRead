import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

class SimpleRead {
    val conf = new SparkConf().setAppName("SimpleRead");
    val sc = new SparkContext(conf);

    def read(file:String) = {
        val lines = sc.textFile(file);
        val dat = lines.map(line=>line.split("\\t"));
        val grp = dat.map(line=>(line(0)+line(1),1)).reduceByKey{ case(x,y)=>x+y };
        for((g,c)<-grp) {
            print(g+" -> "+c+"\n");
        }
    }
}

object Read{
def main(args: Array[String]) = {
        val simpleRead = new SimpleRead;
        simpleRead.read("hdfs://localhost:54310/user/hadoop/nyse-div");
    }
}
