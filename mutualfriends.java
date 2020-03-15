import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class mutualfriends{

    public static void main(String[] args) throws Exception{
		if(args.length!=2) {
			System.out.println("please enter correct commands");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Mutual Friends");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);	
		job.setJarByClass(mutualfriends.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

			public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

				String[] f = values.toString().split("\t");

				if (f.length == 2) {

					int f1ID = Integer.parseInt(f[0]);
					String[] fl = f[1].split(",");
					int f2ID;
					Text tuple_key = new Text();
					for (String f2 : fl) {
						f2ID = Integer.parseInt(f2);

						if((f1ID==0 && f2ID ==1 )||(f1ID==1 && f2ID ==0 )||(f1ID==20 && f2ID ==28193 )||(f1ID==28193 && f2ID ==20 )||(f1ID==1 && f2ID ==29826 )||(f1ID==29826 && f2ID ==1 )||(f1ID==6222 && f2ID ==19272 )||(f1ID==19272 && f2ID ==6222 )||(f1ID==28041 && f2ID == 28056)||(f1ID==28056 && f2ID ==28041)){
							if (f1ID < f2ID) {
								tuple_key.set(f1ID + "," + f2ID);
							} else {
								tuple_key.set(f2ID + "," + f1ID);
							}
							context.write(tuple_key, new Text(f[1]));
						}
					}
				}
			}
		}
    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> fhmp = new HashMap<String,Integer>();
			StringBuilder cfl = new StringBuilder();
			Text cf = new Text();
			for (Text tuples : values) {
				String[] fl = tuples.toString().split(",");
				for (String eachFriend : fl) {
					if(fhmp.containsKey(eachFriend)){
						cfl.append(eachFriend+",");
					}else {
						fhmp.put(eachFriend, 1);
					}
				}
			}
			if(cfl.length()>0) {
				cfl.deleteCharAt(cfl.length()-1);
			}
			cf.set(new Text(cfl.toString()));
			context.write(key, cf);
		}

	}


}