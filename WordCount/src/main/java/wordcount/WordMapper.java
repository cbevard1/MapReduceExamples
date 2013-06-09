package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Since the driver set the input format class to TextInputFormat the mapper gets each line of the
 * file as a seperate input value.
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /**
     * The tokenizer will be used to get our keys
     */
    private StringTokenizer tokenizer = null;

    /**
     * The value we want to return for each key is simply the value 1 and the reducers will take
     * care of summing the result
     */
    private static final IntWritable ONE = new IntWritable(1);

    /**
     * Our writable used for our output key.
     */
    private Text outKey = new Text();

    /**
     * In a nutshell what this code is doing is transforming each line into key value pairs where the value is 1.
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Initialize our tokenizer with the input text string.
        tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            // Set our output key to the word and output it with a count of one.
            outKey.set(tokenizer.nextToken());
            context.write(outKey, ONE);
        }
    }
}