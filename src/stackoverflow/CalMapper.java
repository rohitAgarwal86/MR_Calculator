package stackoverflow;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	Text outkey = new Text();
	IntWritable outvalue = new IntWritable();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("the key is" + key);
		String[] words = value.toString().split(",");
		if (words.length > 1) {
			outkey.set(words[6]);
			outvalue.set(Integer.parseInt(words[3]));
			context.write(outkey, outvalue);
		}

	}

}
