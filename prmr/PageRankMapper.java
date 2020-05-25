package cps.prmr;



import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text, LongWritable, PRWritable> {
	

	private LongWritable outKey = new LongWritable(1);

	private PRWritable outValue = new PRWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		context.getCounter(PRCounters.PAGES_COUNT);
		context.getCounter(PRCounters.DANGLING_PAGE_PR);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {

			String line = value.toString();
			String[] parts = line.split(",");
			if (parts.length < 2) {

				return;
			}

			context.getCounter(PRCounters.PAGES_COUNT).increment(1);
			long pageId = Long.parseLong(parts[0]);
			double pageRank = Double.parseDouble(parts[1]);
			int linkListSize = parts.length - 2;
			if (linkListSize == 0) {

				outKey.set(pageId);
				LLWritable out = new LLWritable(pageId, pageRank, new long[0]);
				outValue.set(out);
				context.write(outKey, outValue);

				context.getCounter(PRCounters.DANGLING_PAGE_PR)
						.increment((long) (pageRank * PageRankJob.DANGLING_PR_FACTOR));
			} else {

				long[] ll = new long[linkListSize];

				double mass = pageRank / linkListSize;
				for (int i = 2; i < parts.length; i++) {
					outKey.set(Long.parseLong(parts[i]));
					outValue.set(new DoubleWritable(mass));
					context.write(outKey, outValue);
					ll[i - 2] = Long.parseLong(parts[i]);
				}

				outKey.set(pageId);
				LLWritable out = new LLWritable(pageId, pageRank, ll);
				outValue.set(out);
				context.write(outKey, outValue);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
