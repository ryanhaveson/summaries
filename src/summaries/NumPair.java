package summaries;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutput;
import java.io.IOException;


public class NumPair implements WritableComparable<NumPair>{

	private DoubleWritable _partialAvg;
	private IntWritable _count;
	
	
	public NumPair() {
		set(new DoubleWritable(), new IntWritable());
	}
	
	public NumPair(double pa, int c){
		set(new DoubleWritable(pa), new IntWritable(c));
	
	}

	private void set(DoubleWritable pa, IntWritable c){
		_partialAvg = pa;
		_count = c;
	}
	
	public DoubleWritable getPartialAvg(){
		return _partialAvg;
	}
	public IntWritable getCount() {
		return _count;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		_partialAvg.readFields(arg0);
		_count.readFields(arg0);
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
			
		_partialAvg.write(arg0);
		_count.write(arg0);
	}

	@Override
	public int compareTo(NumPair o) {
		int cmp = _partialAvg.compareTo(o.getPartialAvg());
		if(cmp != 0) {
			return cmp;
		}
		
		return _count.compareTo(o.getCount());
	
	}
	
	@Override
	public int hashCode() {
		return _partialAvg.hashCode() * 163 + _count.hashCode();
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof NumPair) {
			NumPair np = (NumPair)o;
			return _partialAvg.equals(np.getPartialAvg()) && _count.equals(np.getCount());
		}
		
		return false;
		
	}
	
	@Override
	public String toString() {
		return _partialAvg + "\t" + _count; 
	}
}


