package unstructured;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * The Writable for our output key.
     */
    private Text outKey = new Text();

    /**
     * The Writable for our output value.
     */
    private Text outValue = new Text();

    /**
     * Use a boolean to make sure we don't mix up our metadata
     */
    private boolean inMovieElement = false;

    /**
     * The user name of the movie.
     */
    private String title = "";

    /**
     * The movie director.
     */
    private String director = "";

    /**
     * A cache of the value in string form.
     */
    private String strValue = "";

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        strValue = value.toString();

        try {
            // since the data can be split across multiple nodes make sure we don't start recording
            // on a bad input split
            if (strValue.contains("</movie>")) {
                title = "";
                director = "";
                inMovieElement = false;
            }  else if (strValue.contains("<movie>")) {
                inMovieElement = true;
            } else if (inMovieElement && strValue.contains("<title>") && strValue.contains("</title>")) { // capture the movie title
                title = getXMLElementContents(strValue);
            } else if (inMovieElement && strValue.contains("<director>") && strValue.contains("<director>")) { // capture the movie director's name
                director = getXMLElementContents(strValue);
            }

            // once both variables are set write them and clear our variables again
            if (!title.equals("") && !director.equals("")) {
                outKey.set(director);
                outValue.set(title);
                context.write(outKey, outValue);

                title = "";
                director = "";
            }
        } catch (IndexOutOfBoundsException e) {
            e.printStackTrace();
        }
    }

    /**
     * Basic helper method to parse the value between two xml tags
     * 
     * @param line
     * @return String
     * @throws IndexOutOfBoundsException
     */
    private String getXMLElementContents(String line) throws IndexOutOfBoundsException {
        return line.substring(line.indexOf(">") + 1, line.indexOf("<", line.indexOf(">")));
    }
}
