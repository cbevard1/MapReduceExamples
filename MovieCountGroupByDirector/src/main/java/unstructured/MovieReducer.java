package unstructured;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * The Writable for our output value.
     */
    private Text outValue = new Text();

    /**
     * A string to concatenate our movie titles together.
     */
    private String movieTitles = "";

    /**
     * Number of pages by the author.
     */
    private int numMovies = 0;
    
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException ,InterruptedException {
        // initialize our variables
        numMovies = 0;
        movieTitles = "";
        
        for(Text movie : values) {
            numMovies++;
            movieTitles = movieTitles.concat(movie.toString()).concat(", ");
        }
        
        // finally combined the number of movies and the list of movies into a single value
        outValue.set(numMovies + "\t".concat(movieTitles).substring(0, movieTitles.length() - 1));
        // write our result
        context.write(key, outValue);
    }
}
