package cps.prmr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PRCompleteMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private double danglingPrAve = 0;

	private Text outValue = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		context.getCounter(PRCounters.NON_CONVERGING_PAGES);
		Configuration conf = context.getConfiguration();
		long danglingPr = Long.parseLong(conf.get("DANGLING_PAGE_PR"));
		danglingPrAve = (double) danglingPr / PageRankJob.DANGLING_PR_FACTOR;
		long pageCount = Long.parseLong(conf.get("PAGES_COUNT"));
		danglingPrAve = danglingPrAve / pageCount * PageRankJob.DELTA;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {

			String line = value.toString();
			String[] parts = line.split(",");
			if (parts.length < 3) {
				
				return;
			}

			long pageId = Long.parseLong(parts[0]);
			double prevPr = Double.parseDouble(parts[1]);
			double partialPr = Double.parseDouble(parts[2]);
			double newPr = 0;
			int linkListSize = parts.length - 3;

			newPr = partialPr * PageRankJob.DELTA + 1 - PageRankJob.DELTA + danglingPrAve;
			StringBuffer buf = new StringBuffer();
			buf.append(pageId);
			buf.append(',');
			buf.append(newPr);
			if (linkListSize == 0) {

				outValue.set(buf.toString());
				context.write(null, outValue);
			} else {

				for (int i = 3; i < parts.length; i++) {
					buf.append(',');
					buf.append(Long.parseLong(parts[i]));
				}
				outValue.set(buf.toString());

				context.write(null, outValue);
			}
			double diff = 0;
			if (newPr > prevPr) {
				diff = newPr - prevPr;
			} else {
				diff = prevPr - newPr;
			}
			if (diff > PageRankJob.DIFF) {
				context.getCounter(PRCounters.NON_CONVERGING_PAGES).increment(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
