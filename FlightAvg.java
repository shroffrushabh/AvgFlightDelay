package com;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FlightAvg {
	
	// Global Counter where Counter1 sums the delays
	// and Counter2 sums the total number of delays
	public enum MyCounters {
		Counter1,Counter2
	}

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			
			// We do the following for getting the required data
			// from the input record to the map
			String[] record = value.toString().split(",");

			String flightDate = record[5], origin = record[11], destCity = record[18],
			departureTime = record[26],arrivalTime = record[37],delay = record[39],
			cancelled = record[43],diverted = record[45];

			// We do the following steps for replacing the inverted commas
			// present in the string
			// eg "JFK", "ORD"
			origin=origin.replace("\"", "");
			destCity=destCity.replace("\"", "");
			
			boolean checker = false;
			try {
				String t[]=flightDate.split("-");
				Date dt=new Date(t[0]+"/"+t[1]+"/"+t[2]);
				
				// The following condition will check if the date is in the range
				// i.e. after june 2007 and before may 2008
				checker=dt.after(new Date("2007/05/31")) && dt.before(new Date("2008/06/01"));
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// The initial check for the figuring if the plane took off from "ORD" 
			// and not land at" JFK"
			Boolean flightCheckLeg1=(origin.equals("ORD") && !destCity.equals("JFK"));
			
			// The initial check for the figuring if the plane landed at "JFK" 
			// and did not take off from "ORD"
			Boolean flightCheckLeg2=(!origin.equals("ORD") && destCity.equals("JFK"));
			
			// Additional check to see if flight wasn't diverted of or cancelled
			if ((flightCheckLeg1 || flightCheckLeg2) && checker && 
					cancelled.equals("0.00") && diverted.equals("0.00")) {
				Text Key;

				// We pass the key as a combination of the destCity/origion-flightDate
				if (origin.equals("ORD"))
					Key = new Text(destCity + "-" + flightDate);
				else
					Key = new Text(origin + "-" + flightDate);
				
				// The value is also a combination of the origin-departureT-arrivalTime-
				// delay
				Text val = new Text(origin + "-" + departureTime+ "-" + arrivalTime + "-" + delay);
				context.write(Key, val);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, Text, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			Vector<String> info1 = new Vector<String>();
			Vector<String> info2 = new Vector<String>();
			
			// we create 2 vectors, vector1 stores the flights from ORD
			// vector2 stores the flight from all terminals except ORD
			for (Text val : values) {
				String x = val.toString();
				String[] record = x.split("-");
				if (record[0].equals("ORD")) {
					info1.add(x);
				} else {
					info2.add(x);
				}
			}
			
			// Computing the join here
			// arrivalTime for some terminal should be lesser than
			// the departureTime for JFK
			String[] outer,inner;
			for (int i = 0; i < info1.size(); i++) {
				outer = info1.get(i).split("-");
				for (int j = 0; j < info2.size(); j++) {
					inner = info2.get(j).split("-");
					// The intial check is for entry strings 
					if (!(outer[2].trim().length() == 0 
							|| inner[1].trim().length() == 0)
									&& (Integer.parseInt(outer[2].replace("\"", "")) 
											< Integer.parseInt(inner[1].replace("\"", "")))) {
						result = new FloatWritable(Float.parseFloat(outer[3])
								+ Float.parseFloat(inner[3]));
						// Using the global counter for counting the sum and
						// the total number of records the reducer outputs
						context.getCounter(MyCounters.Counter1).increment
						((long) (Float.parseFloat(outer[3]) + Float.parseFloat(inner[3])));
						context.getCounter(MyCounters.Counter2).increment(1);
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Flight Delay");
		job.setJarByClass(FlightAvg.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(10); // Setting number of reduce tasks to 10
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		if (job.waitForCompletion(true)) {
			System.out.println("Delay is:" + 
					(double)job.getCounters().findCounter(MyCounters.Counter1).getValue()/
					(double)job.getCounters().findCounter(MyCounters.Counter2).getValue());
			System.exit(0);
		}
		System.exit(1);

	}
}



