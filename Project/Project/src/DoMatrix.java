import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by Nikita on 20.03.17.
 */
public class DoMatrix {

    public static class Map extends Mapper<Object, Text, Text, MapWritable> {
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
//            System.out.println("Map");

            String[] columns = values.toString().split(",");
            Text keyText = new Text();
            keyText.set(columns[0]);

            MapWritable map = new MapWritable();

            for (int i = 1; i < columns.length; i++) {

//                System.out.println("map cycle "+ i + " value :" + columns[i].toString() );

                map.put(new IntWritable(i), new Text(columns[i].toString()));
            }
            context.write(keyText, map);
        }
    }


    public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
        public void reduce(Text key, Iterable<MapWritable> maps, Context context) throws IOException, InterruptedException {
//            System.out.println("Reduce");

            String out = new String("");
            for (MapWritable map : maps) {

//                System.out.println("=== New Map ===");

                for (Writable w : map.values()){
                    //Todo There the matrix is building
                    out = out + w.toString();

//                    System.out.println(out);
                }
            }
            context.write(key, new Text(out));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "DoMatrix");
        job.setJarByClass(DoMatrix.class);

        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        String inputFile = "data/Certificates.csv";
        String outputFile = "data/output_" + System.currentTimeMillis();

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
