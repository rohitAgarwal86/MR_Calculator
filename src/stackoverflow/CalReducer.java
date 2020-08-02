package stackoverflow;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CalReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	int sum = 0;
	int count = 0;
	double Avg = 0;

	public void reduce(Text Key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		for (IntWritable val : value) {
			sum = sum + val.get();
			count++;
		}
		if (count > 0) {
			Avg = sum / count;
			context.write(Key, new DoubleWritable(Avg));
		}
	}
}
