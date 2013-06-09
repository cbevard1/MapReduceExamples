package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * The driver for our word count map reduce job
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: [HDFS inputFilePath] [HDFS outputDir]");
            System.exit(-1);
        }

        // Capture our input arguments
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        // delete the old results files if they exist
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outputPath)) {
            System.out.println("Deleting " + outputPath + " from hdfs");
            System.out.println("Delete success: " + fs.delete(outputPath));
        }

        // Create a new Job with the default configuration, named "WordCount"
        Job job = new Job(new Configuration(), "WordCount");

        // Set the jar file via our Driver class. This is so Hadoop can find our user-defined
        // classes.
        job.setJarByClass(WordCountDriver.class);

        // Set the input path of our job using our InputFileFormat -- Note that
        // FileInputFormat.setInputPaths(...) will also work.
        TextInputFormat.setInputPaths(job, inputPath);

        // Set the output path of our job using our OutputFileFormat -- Note that
        // FileOutputFormat.setOutputPath(...) will also work.
        TextOutputFormat.setOutputPath(job, outputPath);

        // Set our Mapper class.
        job.setMapperClass(WordMapper.class);
        // Set our Reducer class.
        job.setReducerClass(SumReducer.class);
        // Because our Reducer is both associative and commutative, we can use our Reducer as our
        // Combiner.
        job.setCombinerClass(SumReducer.class);

        // Set the input format as a TextInputFormat.
        job.setInputFormatClass(TextInputFormat.class);
        // Set the output format as a TextOutputFormat.
        job.setOutputFormatClass(TextOutputFormat.class);
        // The output key of our Mapper and Reducer is a Text writable.
        job.setOutputKeyClass(Text.class);
        // The output value of our Mapper and Reducer is a IntWritable writable.
        job.setOutputValueClass(IntWritable.class);

        // Submit our job and exit.
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
