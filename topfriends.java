import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashMap;



public class topfriends {

	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

			String[] friendslist = values.toString().split("\t");

			if (friendslist.length == 2) {

				int friend1 = Integer.parseInt(friendslist[0]);
				String[] friendsList = friendslist[1].split(",");
				int friend2;
				Text key_val = new Text();
				for (String friend : friendsList) {
					friend2 = Integer.parseInt(friend);
					if (friend2 > friend1) {
						key_val.set(friend1 + "," + friend2);
					} else {
						key_val.set(friend2 + "," + friend1);
					}
					context.write(key_val, new Text(friendslist[1]));
				}
			}
		}
	}

	public static class ReducerClass extends Reducer<Text, Text, Text, IntWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> friendsHashMap = new HashMap<String, Integer>();
			int NumberOfCommonFriends = 0;
			for (Text tuples : values) {
				String[] friendsList = tuples.toString().split(",");
				for (String eachFriend : friendsList) {
					if (friendsHashMap.containsKey(eachFriend)) {
						NumberOfCommonFriends++;
					} else {
						friendsHashMap.put(eachFriend, 1);
					}
				}
			}
			context.write(key, new IntWritable(NumberOfCommonFriends));
		}
	}

	public static class MapperClass1 extends Mapper<Text, Text, LongWritable, Text> {

		private LongWritable count = new LongWritable();

		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			int newVal = Integer.parseInt(values.toString());
			count.set(newVal);
			context.write(count, key);
		}
	}

	public static class ReducerClass1 extends Reducer<LongWritable, Text, Text, LongWritable> {
		 int index = 0;

		public void reduce(LongWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (index < 10) {
					index++;
					context.write(value, key);
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("please enter the correct format to see output");
			System.exit(1);
		}

		{
			Configuration conf1 = new Configuration();
			Job job1d = Job.getInstance(conf1, "Mutual Friends");

			job1d.setJarByClass(topfriends.class);
			job1d.setMapperClass(MapperClass.class);
			job1d.setReducerClass(ReducerClass.class);

			job1d.setMapOutputKeyClass(Text.class);
			job1d.setMapOutputValueClass(Text.class);

			job1d.setOutputKeyClass(Text.class);
			job1d.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job1d, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1d, new Path(args[1]));

			if (!job1d.waitForCompletion(true)) {
				System.exit(1);
			}

			{
				Configuration conf2 = new Configuration();
				Job jobid2 = Job.getInstance(conf2, "Top 10");

				jobid2.setJarByClass(topfriends.class);
				jobid2.setMapperClass(MapperClass1.class);
				jobid2.setReducerClass(ReducerClass1.class);

				
				jobid2.setOutputKeyClass(Text.class);
				jobid2.setOutputValueClass(LongWritable.class);

				jobid2.setMapOutputKeyClass(LongWritable.class);
				jobid2.setMapOutputValueClass(Text.class);

				jobid2.setInputFormatClass(KeyValueTextInputFormat.class);

				jobid2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

				jobid2.setNumReduceTasks(1);

				FileInputFormat.addInputPath(jobid2, new Path(args[1]));
				FileOutputFormat.setOutputPath(jobid2, new Path(args[2]));

				System.exit(jobid2.waitForCompletion(true) ? 0 : 1);
			}
		}
	}
}