package com.nbaranov.Lab2PeopleYouMayKnow;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class PeopleYouMayKnow {

    // Main method.
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // GenericOptionsParser helps parsing attributes passed to the application.
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(PeopleYouMayKnow.class);
        job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntFrendsReducer.class);
        job.setReducerClass(IntFrendsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Adds skip file to cache if available
        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    // Class that will be used for the map tasks.
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable frendOne = new IntWritable();
        private final static IntWritable frendTwo = new IntWritable();
        private Text word = new Text();
        // Specifies whether matching is case sensitive.
        // Value will be set from configuration.
        private boolean caseSensitive;
        // Specifies the patterns that should be ignored in the words.
        // Patterns are contained in files stored in the distributed cache.
        // A configuration parameter specifies whether patter skipping is
        // activated.
        private Set<String> patternsToSkip = new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;

        // Called once at the beginning of the task, i.e.,
        // once for all key-value pairs. Typically used to set
        // configuration parameters and perform other common tasks.
        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            // Read configuration parameters
            conf = context.getConfiguration();
            // First config parameter specifies whether matching will be case sensitive
            caseSensitive = conf.getBoolean("wordcount.case.sensitive", false);
            // Second config parameter specifies the file that contains the patterns to skip
            // It is set by the main method when command-line option ``-skip'' is used.
            if (conf.getBoolean("wordcount.skip.patterns", true)) {
                // This is how file names is retrieved from the distributed cache
                // In this case all the files in the cache are assumed to be skip files
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                if(patternsURIs != null)
                    for (URI patternsURI : patternsURIs) {
                        Path patternsPath = new Path(patternsURI.getPath());
                        String patternsFileName = patternsPath.getName().toString();
                        parseSkipFile(patternsFileName);
                    }
            }
        }

        // This method is called from setup. It parses a skip file
        // and puts the patterns to skip in the patternsToSkip class attribute.
        private void parseSkipFile(String fileName) {
            try {
                // A file in the distributed cache is read as any other file.
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        // Called once for each key/value pair in the input split.
        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            // Igonores case in the line if class attributed was set in setup.
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            // Skips all the patterns on the line
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            StringTokenizer itr = new StringTokenizer(line);
            ArrayList <Integer> frends = new ArrayList<>();
            if (itr.hasMoreTokens){
                itr.nextToken();
                while (itr.hasMoreTokens()) {
                    frendOne.set(Integer.parseInt (itr.nextToken()));
                    for (Integer frend : frends){
                        frendTwo.set(frend);
                        context.write(frendOne, frendTwo);
                        context.write(frendTwo, frendOne);
                        // Gets a counter called ``INPUT_WORDS'' with group ``CountersEnum''
                        Counter counter = context.getCounter(CountersEnum.class.getName(),
                                CountersEnum.INPUT_WORDS.toString());
                        // Increment the counter. Counters are shared among all map tasks.
                        // When the application completes, the counter will contain the total
                        // number of words that have been processed.
                        counter.increment(1);
                    }
                    frends.add(frendOne);
                }
            }
/*      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
        // Gets a counter called ``INPUT_WORDS'' with group ``CountersEnum''
        Counter counter = context.getCounter(CountersEnum.class.getName(),
            CountersEnum.INPUT_WORDS.toString());
            // Increment the counter. Counters are shared among all map tasks.
            // When the application completes, the counter will contain the total
            // number of words that have been processed.
        counter.increment(1);
      }*/
        }

        // Definition for a group of 1 counter that
        // will be used to count all the input words
        static enum CountersEnum { INPUT_WORDS }
    }

    // Reducer. Identical to the initial WordCount example.
    public static class IntFrendsReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        //private IntWritable result = new IntWritable();
        PriorityQueue<Integer> pq = new PriorityQueue<Integer>(defaultSize,Collections.reverseOrder());


        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            ArrayList <Integer> recommended = new ArrayList<>();

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
