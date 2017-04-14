import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.DataFrame;

import org.apache.spark.sql.Row;


public class crollCSV {

    public static void main (String[] args){
        SparkConf conf = new SparkConf().setAppName("NikitaBaranov.Project:CSV Crolling").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        DataFrame df = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("cars.csv");

    }
}
