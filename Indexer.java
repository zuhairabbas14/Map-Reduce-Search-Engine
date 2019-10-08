import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import src.TermFrequency;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;


public class Indexer extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(Indexer.class);
    public final static String FILE_REFERENCE = "CountOfFiles";


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

        int res = ToolRunner.run(new TermFrequency(), args);
        res = ToolRunner.run(new Indexer(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), " TF/Inverse Document Frequency ");
        job.setJarByClass(this.getClass());
        int totalFiles = 0;
        FileSystem fileSystem = FileSystem.get(getConf());
        FileStatus fileStatuses[] = fileSystem.listStatus(new Path(args[0]));
        for (FileStatus temp : fileStatuses) {
            totalFiles += 1;
        }

        job.getConfiguration().set(FILE_REFERENCE, Double.valueOf(totalFiles) + "");

        FileInputFormat.addInputPaths(job, args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord = new Text();

            if (line.isEmpty() == false) {
                String wordsInLine[] = line.split("_");
                String word = wordsInLine[0];

                String wordsInLine2[] = wordsInLine[1].split("\t");
                String fileName = wordsInLine2[0];
                double termFreq = 0.0;
                if (wordsInLine2.length > 1) {
                    termFreq = Double.parseDouble(wordsInLine2[1]);
                }

                currentWord = new Text(fileName + "=" + termFreq);
                context.write(new Text(word), currentWord);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<Text> counts, Context context)
                throws IOException, InterruptedException {
            double termFrequency = 0.0;
            Text textWords = new Text();
            double countOfDocs = 0.0;
            ArrayList<String> wordsInFile = new ArrayList<>();

            double numOfFiles = Double.parseDouble(String.valueOf(context.getConfiguration().get(FILE_REFERENCE)));

            for (Text doc : counts) {
                countOfDocs += 1;
                wordsInFile.add(doc.toString());
            }

            double idfValue = Math.log10(1.0 + (numOfFiles / countOfDocs));

            for (String s : wordsInFile) {
                String temp1[] = s.split("=");
                String fileName = temp1[0];
                termFrequency = Double.parseDouble(temp1[1]);

                textWords = new Text(word.toString() + "_" + fileName);

                context.write(textWords, new DoubleWritable(termFrequency * idfValue));
            }
        }
    }
}
