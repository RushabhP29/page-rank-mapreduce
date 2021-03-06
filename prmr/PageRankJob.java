package cps.prmr;
import cps.prmr.PRCounters;
import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import cps.prmr.FileUtil;
import cps.prmr.PRCompleteMapper;
import cps.prmr.PRCounters;
import cps.prmr.PRWritable;
import cps.prmr.PageRankJob;
import cps.prmr.PageRankMapper;
import cps.prmr.PageRankReducer;

public class PageRankJob {
	

	public static final double DELTA = 0.85;

	public static final int DANGLING_PR_FACTOR = 100000000;

	public static final double NON_CONVERGING_FACTOR = 0.01;

	public static final double DIFF = 0.001;

	public static void main(String args[]) throws IOException {
		if (args.length != 1) {
			
			System.out.println("missing parameter");
			System.exit(0);
		}
		long start = System.currentTimeMillis();
		long ncPageCount = 0;
		int iterationCount = 0;
		int i;
		String inputFile = args[0];
		String outputFile = null;
		try {
			boolean needMoreRounds = true;
			for(i=0;i <= 10;i++) {
				
				//Configuration conf = new Configuration();
				List<Path> inputFiles = FileUtil.getFiles(conf, inputFile);
				//Call Mappper Function as well as the reducer function
				//Use a global variable instead of Context
//				Job job = Job.getInstance(conf, "PageRank-Job-" + iterationCount);
//				job.setJarByClass(PageRankJob.class);
//				job.setInputFormatClass(TextInputFormat.class);
//				for (Path input : inputFiles) {
//					FileInputFormat.addInputPath(job, input);
//				}
//				job.setMapperClass(PageRankMapper.class);
//				job.setMapOutputKeyClass(LongWritable.class);
//				job.setMapOutputValueClass(PRWritable.class);
//
//				job.setReducerClass(PageRankReducer.class);
//				job.setOutputFormatClass(TextOutputFormat.class);
//				outputFile = "/user/hduser/output/" + System.currentTimeMillis();
//				FileOutputFormat.setOutputPath(job, new Path(outputFile));
//				if (!job.waitForCompletion(true)) {
//					System.err.println("PageRank Job Failed");
//				}
//				
//				inputFiles = FileUtil.getFiles(conf, outputFile);
//				
//				Counters counters = job.getCounters();
				PRCounters counters= new PRCounters();
				//Use Object variable to access the counters from the PRCounters
				long danglingPagePr = counters.DANGLING_PAGE_PR);
				long pageCount = counters.PAGES_COUNT;
//				conf = new Configuration();
//				conf.set("DANGLING_PAGE_PR", String.valueOf(danglingPagePr));
//				conf.set("PAGES_COUNT", String.valueOf(pageCount));
				Job prCompleteJob = Job.getInstance(conf, "PRComplete-Job-" + iterationCount);
				prCompleteJob.setJarByClass(PageRankJob.class);
				prCompleteJob.setInputFormatClass(TextInputFormat.class);
				for (Path input : inputFiles) {
					FileInputFormat.addInputPath(prCompleteJob, input);
				}
				prCompleteJob.setMapperClass(PRCompleteMapper.class);
				prCompleteJob.setMapOutputKeyClass(NullWritable.class);
				prCompleteJob.setMapOutputValueClass(Text.class);
				prCompleteJob.setNumReduceTasks(0);
				outputFile = "/user/hduser/output/" + System.currentTimeMillis();
				FileOutputFormat.setOutputPath(prCompleteJob, new Path(outputFile));
				if (!prCompleteJob.waitForCompletion(true)) {
					System.err.println("PRComplete Job Failed");
				}
				inputFile = outputFile;
				counters = prCompleteJob.getCounters();
				ncPageCount = counters.findCounter(PRCounters.NON_CONVERGING_PAGES).getValue();
				iterationCount++;
				if (ncPageCount < pageCount * NON_CONVERGING_FACTOR)
				needMoreRounds = false;
			} while (needMoreRounds);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

}
