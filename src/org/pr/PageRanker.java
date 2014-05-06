package org.pr;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class PageRanker {

	public static class ParserMapper extends Mapper<Object, Text, Text, Text> {
		private static final String baseURL = "hdfs://localhost:9000/wikipedia/in/";
		public void map(Object key, Text value, Context context)
				throws InterruptedException, IOException {
			// get the full path of the page
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			Text currLink = new Text();
			currLink.set(fileSplit.getPath().toString());

			// get all the urls that given page/document(value) has
			List<Text> links = parseLinks(value);

			// write referencer, referencee pairs to context
			for (Text link : links) {
				context.write(currLink, link);
			}
		}

		/*
		 * parse urls from the content of given document and return list of urls
		 */
		private List<Text> parseLinks(Text page) {
			List<Text> urls = new ArrayList<>();
			Document doc = Jsoup.parse(page.toString());
			Elements links = doc.select("a[href]");
			for (Element link : links) {
				String href = link.attr("href");
				if (href.contains("articles") && !href.contains("http://")) {
					try {
						urls.add(new Text(baseURL + URLDecoder.decode(
								href.substring(href.indexOf("articles")),
								"UTF-8")));
					} catch (UnsupportedEncodingException e) {
						e.printStackTrace();
					}
				}
			}
			return urls;
		}

	}
	
	public static class ParserReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> links,
				Context context) throws IOException, InterruptedException {
			String agregatedValue = "1.0";
			for(Text link : links) {
				agregatedValue = agregatedValue + "," + link.toString();
			}
			context.write(key, new Text(agregatedValue));
		}
	}
	
	public static class RankerMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text page, Text rankAndPagesLine, Context context)
				throws InterruptedException, IOException {
			// just to inform that there is such page
			context.write(page, new Text("$"));
			
			String[] rankAndOtherPages = rankAndPagesLine.toString().split(",");
			String otherPages = "";
			int numberOfPages = rankAndOtherPages.length - 1;
			
			// if there is any other page
			// the influence of current page on them
			// output other_page\trank,nuber_of_outgoing_link
			if (numberOfPages > 0) {
				for (int i = 1; i < rankAndOtherPages.length; i++) {
					otherPages = otherPages + "," + rankAndOtherPages[i];
					context.write(new Text(rankAndOtherPages[i]),
							new Text(rankAndOtherPages[0] + "," + numberOfPages));
				}
			}
			
			// also send list of other pages to reducer, to make proper
			// output for mapper for the next iteration
			// we substring otherPages because there is additional comma in the begging
			context.write(page, new Text("#" + otherPages.substring(1)));
		}
	}
	public static class RankerReducer extends Reducer<Text, Text, Text, Text> {
		private static final float damping = 0.85F;
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String otherPages = "";
			boolean isExistingWikiPage = false;
			float sumOfRanks = 0f;

			for (Text value : values) {
				String valueLine = value.toString();

				if (valueLine.equals("$")) {
					isExistingWikiPage = true;
					continue;
				}

				if (valueLine.startsWith("#")) {
					otherPages = valueLine.substring(1);
					continue;
				}

				String[] rankAndNumberOfOutgoingLinks = valueLine.split(",");
				float pageRank = Float.valueOf(rankAndNumberOfOutgoingLinks[0]);
				int numberOfOutgoingLinks = Integer
						.valueOf(rankAndNumberOfOutgoingLinks[1]);

				sumOfRanks += (pageRank / numberOfOutgoingLinks);
			}
			// if given page(key) is not among documents
			// do not try to calculate page rank for it
			if (!isExistingWikiPage) {
				return;
			}
			float newRank = damping * sumOfRanks + (1 - damping);

			context.write(key, new Text(newRank + "," + otherPages));
		}
	}
	
	public static class SorterMapper extends Mapper<Text, Text, FloatWritable, Text> {
		public void map(Text page, Text rankAndPagesLine, Context context)
				throws InterruptedException, IOException {
			float rank = Float.valueOf(rankAndPagesLine.toString().split(",")[0]);
			context.write(new FloatWritable(rank), page);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherOptions = new GenericOptionsParser(conf, args).getRemainingArgs();
		int numberOfIterations = 4;
		String operation = "both";
		String pathPrefix = "/wikipedia/";
		String inputPath = pathPrefix + "in";
		
		if (otherOptions.length == 1) {
			//i.e, args: 7
			numberOfIterations = Integer.parseInt(otherOptions[0]);
		} else if (otherOptions.length == 2) {
			//i.e, args: 7 ranking
			numberOfIterations = Integer.parseInt(otherOptions[0]);
			operation = otherOptions[1];
		} else if (otherOptions.length == 3) {
			//i.e, args: 7 both /wikipedia/articles/a
			numberOfIterations = Integer.parseInt(otherOptions[0]);
			operation = otherOptions[1];
			inputPath = otherOptions[2];
		}
		
		if (operation.equals("both") || operation.equals("parsing")) {
			runParserJob(conf, new Path(inputPath), new Path(pathPrefix
					+ "ranking/iter00"));
		}

		if (operation.equals("both") || operation.equals("ranking")) {
			int runs = 0;
			NumberFormat nf = new DecimalFormat("00");
			// run ranker job multiple times to get approximated page rank
			// values
			for (; runs < numberOfIterations; runs++) {
				runRankerJob(
						conf,
						new Path(pathPrefix + "ranking/iter" + nf.format(runs)),
						new Path(pathPrefix + "ranking/iter"
								+ nf.format(runs + 1)));
			}

			// sort pages according to their ranks
			runSorterJob(conf,
					new Path(pathPrefix + "ranking/iter" + nf.format(runs)),
					new Path(pathPrefix + "ranking/ordered_result"));
		}
	}
	
	private static void runParserJob(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);

		job.setJarByClass(PageRanker.class);

		job.setMapperClass(ParserMapper.class);
		job.setReducerClass(ParserReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = input.getFileSystem(conf);
		addInputPathRecursively(job, fs, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}
	
	private static void runRankerJob(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);

		job.setJarByClass(PageRanker.class);

		job.setMapperClass(RankerMapper.class);
		job.setReducerClass(RankerReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}
	
	private static void runSorterJob(Configuration conf, Path input, Path output)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf);

		job.setJarByClass(PageRanker.class);

		job.setMapperClass(SorterMapper.class);

		job.setSortComparatorClass(ReverseFloatComparator.class);
		
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(true);
	}

	private static void addInputPathRecursively(Job job, FileSystem fs,
			Path path) throws IOException {
		RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(path);
		while (iter.hasNext()) {
			LocatedFileStatus stat = iter.next();
			if (stat.isDirectory()) {
				addInputPathRecursively(job, fs, stat.getPath());
			} else {
				FileInputFormat.addInputPath(job, stat.getPath());
			}

		}
	}

	public static class ReverseFloatComparator extends WritableComparator {

	    public ReverseFloatComparator() {
	        super(FloatWritable.class);
	    }

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

			float thisValue = readFloat(b1, s1);
			float thatValue = readFloat(b2, s2);
			return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0
					: -1));
		}
	}
}