package summaries;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class Reduce extends Reducer<Text, NumPair, Text, DoubleWritable> {
	@Override
	public void reduce(final Text key, final Iterable<NumPair> values, final Context context)
		throws IOException, InterruptedException {
		
		Double sum = 0.0;
		Integer count = 0;
		for (NumPair v : values ) {
			sum += v.getPartialAvg().get();
			count += v.getCount().get();
		}
		Double ratio = sum/count;
		context.write(key, new DoubleWritable(ratio));
	}
}
