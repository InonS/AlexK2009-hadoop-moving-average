package com.hubspaces.alexk2009;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Calculating moving averages with Hadoop and Map-Reduce
 * 
 * http://alexk2009.hubpages.com/hub/Calculating-moving-averages-with-Hadoop-and
 * -Map-Reduce
 * 
 * @author administrator
 * 
 * @since 2015-06-23
 * 
 */
public class HadoopMovingAverage {

	// For production the windowlength would be a commandline or other argument
	static double windowlength = 3.0;

	static int thekey = (int) windowlength / 2;

	// used for handling the circular list.
	static boolean initialised = false;

	// Sample window
	static ArrayList<Double> window = new ArrayList<Double>();

	// The Map method processes the data one point at a time and passes the
	// circular list to the reducer.
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			double wlen = windowlength;

			// creates windows of samples and sends them to the Reducer
			partitionData(value, output, wlen);
		}

		// Create sample windows starting at each sata point and sends them to
		// the reducer
		private void partitionData(Text value,
				OutputCollector<Text, Text> output, double wlen)
				throws IOException {
			String line = value.toString();

			// the division must be done this way in the mapper.
			Double ival = new Double(line) / wlen;

			// Build initial sample window
			if (window.size() < windowlength) {
				window.add(ival);
			}

			// emit first window
			if (!initialised && window.size() == windowlength) {
				initialised = true;
				emit(thekey, window, output);
				thekey++;
				return;
			}

			// Update and emit subsequent windows
			if (initialised) {

				// remove oldest datum
				window.remove(0);

				// add new datum
				window.add(ival);
				emit(thekey, window, output);
				thekey++;
			}
		}
	}

	// Transform list to a string and send to reducer. Text to be replaced by
	// ObjectWritable
	// Problem: Hadoop apparently requires all output formats to be the same so
	// cannot make this output collector differ from the one the reducer uses.
	public static void emit(int key, ArrayList<Double> value,
			OutputCollector<Text, Text> output) throws IOException {
		Text tx = new Text();
		tx.set(new Integer(key).toString());

		String outstring = value.toString();

		// remove the square brackets Java puts in
		String tidied = outstring.substring(1, outstring.length() - 1).trim();

		Text out = new Text();
		out.set(tidied);

		output.collect(tx, out);
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			while (values.hasNext()) {
				computeAverage(key, values, output);

			}

		}

		// computes the average of each window and sends to ouptut collector.
		private void computeAverage(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output) throws IOException {
			double sum = 0;
			String thevalue = values.next().toString();
			String[] thenumbers = thevalue.split(",");
			for (String temp : thenumbers) {

				// need to trim the string because the constructor does not
				// trim.
				Double ds = new Double(temp.trim());
				sum += ds;
			}
			Text out = new Text();
			String outstring = Double.toString(sum);
			out.set(outstring);
			output.collect(key, out);
		}
	}

	/**
	 * @param args
	 *            command-line arguments...
	 */
	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf(HadoopMovingAverage.class);
		/*
		HadoopMovingAverage myHadoopMovingAverage = new HadoopMovingAverage();
		JobConf conf = new JobConf(myHadoopMovingAverage.getClass());
		*/
		conf.setJarByClass(HadoopMovingAverage.class);
		
		conf.setJobName("HadoopMovingAverage");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		FileInputFormat
				.setInputPaths(conf, new Path("input/movingaverage.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("output/smoothed"));

		JobClient.runJob(conf);
	}
}
