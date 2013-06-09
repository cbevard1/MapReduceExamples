package structured;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class StructuredDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("usage: [HDFS inputFilePath] [HDFS outputDir]");
            System.exit(-1);
        }

        System.out.println("Input File: " + args[0]);

        // Capture our input arguments. These should be URIs for HDFS
        Path inFile = new Path(args[0]);
        Path outDir = new Path(args[1]);

        // Delete the old result files if they exist
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(outDir)) {
            System.out.println("Deleting " + outDir + " from HDFS");
            System.out.println("Delete success: " + fs.delete(outDir));
        }

        // Set up job configuration
        Job job = new Job(new Configuration(), "Structured Job");
        job.setJarByClass(StructuredDriver.class);

        // set the input and output paths
        TextInputFormat.setInputPaths(job, inFile);
        TextOutputFormat.setOutputPath(job, outDir);

        // set the input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set the mapper and reducer for the job
        job.setMapperClass(StudentMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Submit our job and exit.
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
