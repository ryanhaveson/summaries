package summaries;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Map extends Mapper<LongWritable, Text, Text, NumPair>{

	@Override
	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(",");
		
		try {
			String maritalStatus = data[5];
			Double hrs = Double.parseDouble(data[12]);
		
			context.write(new Text(maritalStatus), new NumPair(hrs,1));
		} catch (Exception e){
			// HANDLE IT
		}
		
	}
	
}
