package src;

import java.io.IOException;
import java.net.URI;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.parser.ParseException;

public class WordCounting extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(WordCounting.class);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://10.90.138.32:9000"), conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        int res = ToolRunner.run(new WordCounting(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " docwordcount ");
        job.setJarByClass(this.getClass());


        FileInputFormat.addInputPaths(job, args[0]);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString().toLowerCase();
            JSONParser parser = new JSONParser();
            JSONObject json = null;
            try {
                json = (JSONObject) parser.parse(line);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            line = json.get("text").toString();

            Text currentWord = new Text();


            FileSplit fs = (FileSplit) context.getInputSplit();
            String file = fs.getPath().getName();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }
                currentWord = new Text(word.toLowerCase() + "_" + file);

                context.write(currentWord, one);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable count : counts) {
                sum += count.get();
            }

            context.write(word, new IntWritable(sum));
        }
    }
}

