package com.mr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class MapTest {
	private static final Logger log = Logger.getLogger(MapTest.class.getName());

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Job job = new Job(conf, "mapTest");
		job.setJarByClass(MapTest.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(CollectionReducer.class);
		job.setReducerClass(CollectionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileSystem fs = FileSystem.get(conf);
		List<Path> pathList = getAllGZPath(fs, new Path(otherArgs[0]));
		if (pathList == null || pathList.size() < 1) {
			log.error("路径错误！ path = " + otherArgs[0]);
			System.exit(1);
		}
		for (Path path : pathList) {
			FileInputFormat.addInputPath(job, path);
		}

		Path p = new Path(otherArgs[1]);
		if (fs.exists(p)) {
			fs.delete(p, true);
			System.out.println("输出路径存在，已删除！");
		}
		// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, p);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	static public List<Path> getAllGZPath(FileSystem fs, Path path) {
		List<Path> pathList = new ArrayList<Path>();

		try {

			if (fs.isFile(path)) {
				// if (path.getName().contains("part-")) {
				// pathList.add(path);
				// }
				pathList.add(path);
				return pathList;
			}

			FileStatus[] fileStatus = fs.listStatus(path);
			for (int i = 0; i < fileStatus.length; i++) {
				FileStatus fileStatu = fileStatus[i];
				Path item_path = fileStatu.getPath();
				if (fs.isDirectory(item_path)) {
					List<Path> list = getAllGZPath(fs, item_path);
					if (list != null && list.size() > 0) {
						pathList.addAll(list);
					}
				} else if (fs.isFile(item_path)) {
					// pathList.add(item_path);
					// if (item_path.getName().contains("part-")) {
					// pathList.add(item_path);
					// }
					pathList.add(item_path);
				}
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.error(e);
			pathList = null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			log.error(e);
			pathList = null;
		}
		return pathList;
	}

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {

		static String pt = null;

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			// String path = ((FileSplit) context.getInputSplit()).getPath()
			// .toString();

			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();

			String el = "\\d{4}-[0,1][0-9]-[0-3][0-9]";
			Pattern pattern = Pattern.compile(el);
			Matcher matcher = pattern.matcher(path);
			if (matcher.find()) {
				System.out.println(matcher.group());
			}

			// long start = fileSplit.getStart();
			// log.error("path = " + path.toString() + " start = " + start);
		}

		private String getDateFromPath(
				org.apache.hadoop.mapreduce.Mapper.Context context) {
			String dt = null;
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String path = fileSplit.getPath().toString();

			String el = "\\d{4}-[0,1][0-9]-[0-3][0-9]";
			Pattern pattern = Pattern.compile(el);
			Matcher matcher = pattern.matcher(path);
			if (matcher.find()) {
				dt = matcher.group();
			}
			return dt;
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

		}

		@Override
		protected void cleanup(
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		}

		@Override
		public void run(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			setup(context);
			cleanup(context);
		}

	}

	public static class CollectionReducer extends
			Reducer<Text, Text, Text, Text> {
		protected void reduce(Text arg0, Iterable<Text> arg1,
				org.apache.hadoop.mapreduce.Reducer.Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, arg2);
		}

	}
}
