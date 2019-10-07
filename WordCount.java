
import java.util.*;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import sun.security.provider.ConfigFile;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class WordCount {

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        
        public void enumerator(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);


            while (tokenizer.hasMoreTokens()) {

            }
        }
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // example: this is edureka class 3 is

            // key: byte offset -
            // value: this is edureka class 3 is

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);


            while (tokenizer.hasMoreTokens()) {
                value.set(tokenizer.nextToken());
                // this
                // is
                // edureka
                // ..

                context.write(value, new IntWritable(1));
                // this, 1
                // is, 1
                // edureka, 1
                // class, 1
                // 3, 1
                // is, 1
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // this, (1)
            // is, (1, 1)
            // edureka, (1)
            // class, (1)
            int sum=0;
            for (IntWritable x: values) {
                sum += x.get();
                // sum = 1
                // sum = 2
                // sum = 1 ...
            }
            context.write(key, new IntWritable(sum));
            // this, 1
            // is, 2
            // edureka, 1       ...
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        
        Job job = Job.getInstance(conf, "WordCount");

        job.setJarByClass(WordCount.class);
        job.setMapper(Map.class);
        job.setReducer(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        java.nio.file.Path outputPath = new Path(args[1]);

        // configuring the input/output path from the filesystem into the job
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.seteOutputPath(job, new Path(args[1]));

        // hadoop jar wordcount.jar /input /output
        // input and output directories as two arguments

        outputPath.getFileSystem(conf).delete(outputPath, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}