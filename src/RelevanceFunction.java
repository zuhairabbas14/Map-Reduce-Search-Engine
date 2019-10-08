import java.io.IOException;

import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.io.Text;


public class RelevanceFunction extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(RelevanceFunction.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new RelevanceFunction(), args);

        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " Relevance function applying ");
        job.setJarByClass(this.getClass());
        //Get the search query
        job.getConfiguration().set("Query", args[4]);
        //Get the path of the Input folder from the console
        FileInputFormat.addInputPaths(job, args[0]);
        //Get the path of the Output folder from the console
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord = new Text();

            if (line.isEmpty() == false) {
                String wordsInLine[] = line.split("___");
                String word = wordsInLine[0];
                String searchWords[] = context.getConfiguration().get("Query").split(" ");
                for (String s : searchWords) {
                    if (word.equalsIgnoreCase(s)) {
                        String wordsInLine2[] = wordsInLine[1].split("\t");
                        String fileName = wordsInLine2[0];
                        double termFreq = Double.parseDouble(wordsInLine2[1]);
                        currentWord = new Text(fileName);
                        context.write(currentWord, new DoubleWritable(termFreq));
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<DoubleWritable> counts, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable count : counts) {
                sum += count.get();
            }
            context.write(word, new DoubleWritable(sum));
        }
    }
}
