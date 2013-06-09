package wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The word count reducer's only job is to sum the value returned from the mapper (1) for each key
 */
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    /**
     * The value we are going to output is going to be an IntWritable
     */
    private IntWritable outValue = new IntWritable();

    /**
     * The sum of our values.
     */
    private int sum;

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;

        /*
         * This code is simply taking in the key values from our mapper
         * <key, value>
         * <whale, 1>
         * <whale, 1>
         * <whale, 1>
         * <whale, 1>
         * and reducing them to <whale, 4>
         */
        for (IntWritable i : values) {
            sum += i.get();
        }

        outValue.set(sum);
        context.write(key, outValue);
    }
}
