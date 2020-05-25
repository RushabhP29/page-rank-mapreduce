package cps.prmr;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {
	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(long[] longValues) {
		super(LongWritable.class);
		LongWritable[] values = new LongWritable[longValues.length];
		for (int i = 0; i < longValues.length; i++) {
			values[i] = new LongWritable(longValues[i]);
		}
		set(values);
	}
}