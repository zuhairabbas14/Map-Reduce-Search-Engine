import java.io.IOException;
import java.net.URI;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import search_engine.RelevanceFunction;

public class Query extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Query.class);
    private static int limit;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(URI.create("hdfs://10.90.138.32:9000"), conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        output = new Path(args[2]);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        limit = Integer.parseInt(args[3]);
        int res = ToolRunner.run(new RelevanceFunction(), args);
        res = ToolRunner.run(new Query(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " Top Relevance ");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString().toLowerCase();
            Text currentWord = new Text();

            String wordsInLine[] = line.split("\t");
            String fileName = wordsInLine[0];
            double score = Double.parseDouble(wordsInLine[1]);
            currentWord = new Text(fileName);
            context.write(new DoubleWritable(score), currentWord);

        }
    }

    public static class Reduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        public void reduce(DoubleWritable words, Iterable<Text> counts, Context context)
                throws IOException, InterruptedException {
            int i = 0;
            for (Text t : counts) {
                if (i > limit) break;
                i++;
                context.write(t, words);
            }
        }
    }

    public static class DescendingKeyComparator extends WritableComparator {
        protected DescendingKeyComparator() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(byte[] firstArray, int x1, int y1, byte[] secondArray, int x2, int y2) {
            double x = WritableComparator.readDouble(firstArray, x1);
            double y = WritableComparator.readDouble(secondArray, x2);
            if (x > y)
                return -1;
            else if (x < y)
                return 1;
            return 0;
        }
    }
}
