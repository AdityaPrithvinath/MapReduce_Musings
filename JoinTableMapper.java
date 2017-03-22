package part.three;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
/*import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;*/
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;



public class JoinTableMapper extends Configured implements Tool{
	public static class PageViewMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens[0].equals("page_id"))         // identify the header line
				context.write(new Text("#"), new Text("1,"+tokens[1]+","+ tokens[0]+","+tokens[2]));  // <"#","1,page_id">
			else
				context.write(new Text(tokens[1]), new Text("1," + tokens[0]+","+tokens[2])); // <user_id,"1,"+page_id>
		}
	}
	public static class UserMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens[0].equals("user_id"))         // identify the header line
				context.write(new Text("#"), new Text("2," + tokens[1]));  // <"#","0,age">
			else
				context.write(new Text(tokens[0]), new Text("2," + tokens[1])); // <user_id,"0,"+age>
		}
	}
	public static class UserGenderMapper extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\t");
			if (tokens[0].equals("user_id"))         // identify the header line
				context.write(new Text("#"), new Text("3," + tokens[1]));  // <"#","0,gender">
			else
				context.write(new Text(tokens[0]), new Text("3," + tokens[1])); // <user_id,"0,"+age>
		}
	}
	public static class JoinTableReducer extends Reducer<Text,Text,Text,Text>{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			ArrayList<String> obj=new ArrayList<String>();
			Text t=new Text();
			Text check=new Text();
			String age=new String();
			String gender=new String();
			check.set("#");
			StringBuilder s=new StringBuilder();
			for(Text val: values){
				String[] valTok=val.toString().split(",");
				System.out.println("Keyyyyy : "+key.toString()+"Valyes :"+val.toString());
				if(key.equals(check)){
					if(valTok[0].equals("1")){
						s.append(valTok[2].toString()+"\t"+valTok[3].toString());
						//s=valTok[2].toString()+"\t"+valTok[3].toString();
						//System.out.println(s.toString());
						//header.set(valTok[1].toString());
					}
					else if (valTok[0].equals("2"))
						s.append(valTok[1].toString());
						//s=s.concat("\t"+valTok[1].toString());
					else if (valTok[0].equals("3")){
						s.append("\t"+valTok[1].toString()+"\t");
						//s =s.concat("\t"+valTok[1].toString());
						
					}
				
				}
				else
				{
					
					if(valTok[0].equals("1"))
						obj.add(valTok[1].toString()+"\t"+valTok[2].toString());
						//s = valTok[1].toString()+"\t"+valTok[2].toString();
					else if (valTok[0].equals("2"))
						age=valTok[1].toString();
						//s =s.concat("\t"+valTok[1].toString());
					else if (valTok[0].equals("3")){
						//s =s.concat("\t"+valTok[1].toString());
						gender=valTok[1].toString();
						//key.set(valTok[1].toString());
					}

				}

			}
			//System.out.println(s.toString()+":this is:"+key.toString());
			if(key.equals(check)){
				key.set("user_id");
			
			t.set(s.toString());
			context.write(key,t);
			}
			else
			{
				String temp= new String();
				for(int i=0;i<obj.size();i++){
					temp=age+"\t"+gender+"\t"+obj.get(i);
					t.set(temp);
					//System.out.println("KEy: " +key.toString()+" S is : "+temp);
					context.write(key, t);
				}
			}
		}



	}


	@Override
	public int run(String[] args) throws Exception{
		if(args.length < 4){
			System.err.println("Usage <input path1> <input path2> <input path3> <outputpath>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Join Tables");
		job.setJarByClass(JoinTableMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PageViewMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, UserMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, UserGenderMapper.class);
		//job.setMapperClass(JoinTableMapper.class);
		job.setReducerClass(JoinTableReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileSystem.get(conf).delete(new Path(args[3]), true);
		FileOutputFormat.setOutputPath(job, new Path(args[3]));
		return(job.waitForCompletion(true) ? 0 : 1);

	}



	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		JoinTableMapper driver = new JoinTableMapper();
		int exitCode = ToolRunner.run(driver, args);
		System.exit(exitCode);


	}

}