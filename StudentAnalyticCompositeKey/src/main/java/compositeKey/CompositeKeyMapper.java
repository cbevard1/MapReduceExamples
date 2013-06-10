package compositeKey;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CompositeKeyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * A String array to store our tab delimited values
     */
    private String[] tokens;

    /**
     * An IntWritable used for output value of our mapper
     */
    private static final IntWritable ONE = new IntWritable(1);

    /**
     * A Text object used as the output key for our mapper
     */
    private Text outKey = new Text();

    // constants to help identify columns in our tab delimited file
    protected static final int GENDER = 0;
    protected static final int GRADE = 1;
    protected static final int AGE = 2;
    protected static final int ETHNICITY = 3;
    protected static final int LOCATION = 4;
    protected static final int SCHOOL = 5;
    protected static final int GRADES_IMPORTANCE = 6;
    protected static final int SPORTS_IMPORTANCE = 7;
    protected static final int LOOKS_IMPORTANCE = 8;
    protected static final int MONEY_IMPORTANCE = 9;
    protected static final int NUM_FIELDS = 10;

    /**
     * Create a composite key in the format school:grade:gender:most_important with a value of 1 to
     * be counted by the reducer. Creating a key in this format can be useful when working with
     * distributed databases such as HBase and Accumulo, so this example is more relavent than it
     * may initially seem.
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        tokens = value.toString().split("\t");
        
        // if the data is bad throw this input split away
        if(tokens.length != NUM_FIELDS) {
            return;
        }
        
        String most_important = "";
        if (Integer.parseInt(tokens[GRADES_IMPORTANCE]) == 1) {
            most_important = "Grades";
        } else if (Integer.parseInt(tokens[SPORTS_IMPORTANCE]) == 1) {
            most_important = "Sports";
        } else if (Integer.parseInt(tokens[LOOKS_IMPORTANCE]) == 1) {
            most_important = "Looks";
        } else if (Integer.parseInt(tokens[MONEY_IMPORTANCE]) == 1) {
            most_important = "Money";
        }
        
        // now that we've gathered the information for the key build it and write the output <key, value>
        outKey.set(tokens[SCHOOL] + ":" + tokens[GRADE] + ":" + tokens[GENDER] + ":" + most_important);
        context.write(outKey, ONE);
    }
}
