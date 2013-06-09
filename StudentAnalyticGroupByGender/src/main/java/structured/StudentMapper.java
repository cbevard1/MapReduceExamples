package structured;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StudentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
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
    private Text outkey = new Text();

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
     * Get a count of the students grouped by gender at Brentwood Middle school who rank looks as
     * being most important to them
     */
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        tokens = value.toString().split("\t");
        if (tokens.length == NUM_FIELDS && tokens[SCHOOL].equals("Brentwood Middle") && Integer.parseInt(tokens[LOOKS_IMPORTANCE]) == 1) {
            outkey.set(tokens[GENDER]);
            context.write(outkey, ONE);
        }
    }
}
