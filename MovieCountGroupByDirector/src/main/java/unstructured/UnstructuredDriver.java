package unstructured;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author cbevard1 Here we are creating a driver to run a map reduce job on an unstructured xml
 *         file. The key difference from previous examples is that the output value class is now
 *         Text instead of IntWritable.
 */
public class UnstructuredDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("usage: [HDFS inputFilePath] [HDFS outputDir]");
            System.exit(-1);
        }

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
        Job job = new Job(new Configuration(), "Unstructured Job");
        job.setJarByClass(UnstructuredDriver.class);

        // set the input and output paths
        TextInputFormat.setInputPaths(job, inFile);
        TextOutputFormat.setOutputPath(job, outDir);

        // set the input and output format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the mapper and reducer for the job
        job.setMapperClass(MovieMapper.class);
        job.setReducerClass(MovieReducer.class);

        // Submit our job and exit.
        System.exit(job.waitForCompletion(true) ? 0 : -1);
    }
}
