package cps.prmr;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PageRankReducer extends Reducer<LongWritable, PRWritable, NullWritable, Text> {
	// private static final Log log = LogFactory.getLog(PageRankReducer.class);
	private Text value = new Text();

	@Override
	protected void reduce(LongWritable key, Iterable<PRWritable> values, Context context)
			throws IOException, InterruptedException {
		long src = key.get();
		double sum = 0;
		double prevPr = 0;
		StringBuffer buf = new StringBuffer();
		for (PRWritable value : values) {
			Writable v = value.get();
			if (v instanceof DoubleWritable) {
				DoubleWritable dv = (DoubleWritable) v;
				sum += dv.get();
			} else {
				LLWritable llwv = (LLWritable) v;
				prevPr = llwv.getPrevPR().get();
				Writable[] longValues = llwv.getLinkList().get();
				if (longValues != null && longValues.length > 0) {
					for (int i = 0; i < longValues.length; i++) {
						LongWritable destId = (LongWritable) longValues[i];
						buf.append("," + destId.get());
					}
				}
			}
		}
		value.set(src + "," + prevPr + "," + sum + buf.toString());
		context.write(null, value);
	}
}
