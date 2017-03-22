package One;


import java.io.IOException;
import java.util.StringTokenizer;
//import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
/*import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;*/
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;





public class FirstCount {

	public static class FirstLetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		String line = new String();
		Text firstLetter = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());

			while (itr.hasMoreTokens()) {
				String se=itr.nextToken();
				
				if(!se.substring(0,1).matches("[a-z]")){
					for(char c : se.toCharArray()){
						if(Character.isLetter(c)){
							firstLetter.set(String.valueOf(c));
							context.write(firstLetter, one);
							break;
						}
					}
					
				}
				else{
					firstLetter.set(se.substring(0,1));
					context.write(firstLetter, one);
				}
			}
		}
	}

	public static class FirstLetterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException
		{

			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "First Count");
		job.setJarByClass(FirstCount.class);
		job.setMapperClass(FirstLetterMapper.class);
		job.setCombinerClass(FirstLetterReducer.class);
		job.setReducerClass(FirstLetterReducer.class);
		/*job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);*/
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}

}
