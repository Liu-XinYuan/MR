package com.mr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class DataCollection {
	private static final Logger log = Logger.getLogger(DataCollection.class
			.getName());

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();

		conf.setStrings("encoding", args[2]);

		// conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx819m");
		// conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",
		// true);
		// conf.set("fs.defaultFS", "hdfs://nameservice1");
		// conf.set("mapreduce.framework.name", "yarn");
		// // conf.set("yarn.resourcemanager.address", "master:8032");
		// conf.set("mapred.remote.os", "Linux");

		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// if (otherArgs.length != 3) {
		// log.error("参数传递错误");
		// System.exit(3);
		// }
		// String outputDir = null;
		// if (!otherArgs[2].equals("0")) {
		// outputDir = otherArgs[2];
		// }

		// @SuppressWarnings("deprecation")
		// JobConf jobConf = new JobConf(conf);
		Job job = new Job(conf, "dc_real");
		job.setJarByClass(DataCollection.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(CollectionReducer.class);
		job.setReducerClass(CollectionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// job.setNumReduceTasks(10); // 设置reduce个数
		// job.setSortComparatorClass();
		// job.setSortComparatorClass();

		// MultipleOutputs.addNamedOutput(job, "all", TextOutputFormat.class,
		// Text.class, Text.class);

		MultipleOutputs.addNamedOutput(job, "base", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "loc", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "pc", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "apklist", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "apkhis", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "apkuse", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "apkdef", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "apkf", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "gps", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "blue", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "url", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "phonelst", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "schedule", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "sdcard", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "earphone", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "charging", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "cable", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "phonerec", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "photolst", TextOutputFormat.class,
				Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "messagecon",
				TextOutputFormat.class, Text.class, Text.class);

		FileSystem fs = FileSystem.get(conf);
		Path p = new Path(otherArgs[1]);
		if (fs.exists(p)) {
			fs.delete(p, true);
			System.out.println("输出路径存在，已删除！");

		}

		/**
		 * 文件输入 应先解压 再输入解压后的目标文件
		 */
		List<Path> gzList = getAllGZPath(fs, new Path(otherArgs[0]));
		log.info(String.format("gzlistNum = %s", gzList.size()));
		for (Path path : gzList) {
			log.info(String.format("part = %s", path.toString()));
			// System.out.println(String.format("part = %s", path.toString()));
			// FileInputFormat.addInputPath(job, path);
			TextInputFormat.addInputPath(job, path);
		}

		// TextInputFormat.addInputPath(job, new Path(otherArgs[0]));

		FileOutputFormat.setOutputPath(job, p);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static void uncompress1(String uri) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);

		Path inputPath = new Path(uri);
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		CompressionCodec codec = factory.getCodec(inputPath);
		if (codec == null) {
			System.out.println("no codec found for " + uri);
			System.exit(1);
		}
		String outputUri = CompressionCodecFactory.removeSuffix(uri,
				codec.getDefaultExtension());
		InputStream in = null;
		OutputStream out = null;
		try {
			in = codec.createInputStream(fs.open(inputPath));
			out = fs.create(new Path(outputUri));
			IOUtils.copyBytes(in, out, conf);
		} finally {
			IOUtils.closeStream(out);
			IOUtils.closeStream(in);
		}
	}

	static public List<Path> getAllGZPath(FileSystem fs, Path path) {
		List<Path> pathList = new ArrayList<Path>();

		try {

			if (fs.isFile(path)) {
				if (path.getName().contains("part-")) {
					pathList.add(path);
				}
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
					if (item_path.getName().contains("part-")) {
						pathList.add(item_path);
					}
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

	// public static void main(String[] args) {
	// String str = IntSumReducer.stampToDate("1469238905278");
	// System.out.println(str);
	//
	// }
	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, Text> {
		public static String valueString = null;
		public static String flag = null;
		public static String ac = null;

		public static String encoding = "UTF-8";

		@Override
		protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			String encodingString = context.getConfiguration().get("encoding");
			if (encodingString != null) {
				encoding = encodingString;
			}
			log.info("encoding ：" + encoding);
		}

		/**
		 * {"a03":"0oavmbzh9O7hYmvhaN086vCQxJcdN3rJlQDgAqkK","a04":
		 * "SecurityCenter","a05":"V5.2.1_380","ac":
		 * "com.zhuoyi.security.ps.PowerSaving_Config_Dialog"
		 * ,"did":"7bdb0009-30db-4b60-8f62-b1faac6f923c"
		 * ,"du":1969,"et":1479360309628
		 * ,"sid":"79ee24302e9c79b0ad82e04b8d1f4f1b","st":1479360307659}
		 */
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			if (value == null) {
				return;
			}
			String line = new String(value.getBytes(), 0, value.getLength(),
					encoding);

			// String line = new String(value.toString().getBytes(encoding),
			// encoding);
			// String line = new String(value.getBytes(), encoding).trim();
			// String line = value.toString().trim();
			int startIndex = line.indexOf("{");
			int stopIndex = line.lastIndexOf("}");
			if (startIndex == -1 || stopIndex == -1) {
				log.error("错误字符串： " + line);
				return;
			}

			try {
				line = line.substring(startIndex, stopIndex + 1);
				JSONObject jsonObject = new JSONObject(line);

				/**
				 * 将ac作为reduce 的 key json_line 作为 value
				 */
				ac = jsonObject.getString("ac");
				if (ac == null) {
					return;
				}
				if (ac.equals("head")) {
					String imsi = jsonObject.getString("im");
					String imei = jsonObject.getString("is");
					String uuid = jsonObject.getString("uuid");
					if (!(imsi + imei + uuid).equals("")) {
						flag = (imsi + "_" + imei + "_" + uuid).trim();
					}
				}
				valueString = flag + "\t" + line;
				context.write(new Text(ac), new Text(valueString));

			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error(e);
			}
		}
	}
	
	

	public static class CollectionReducer extends
			Reducer<Text, Text, Text, Text> {

		private MultipleOutputs<Text, Text> mos;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
			mos = new MultipleOutputs<Text, Text>(context);

		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {

				// mos.write("all", key, value);

				String line = value.toString().trim();
				String[] baseInfo = baseInfoCL(line);
				if (baseInfo == null) {
					log.error("baseInfo parse error");
					continue;
				}
				JSONObject jsonObject = jsonParse(line);
				if (jsonObject == null) {
					log.error("jsonObject is null");
					continue;
				}
				try {
					String ac = jsonObject.getString("ac");
					if (ac == null) {
						continue;
					}
					ac = ac.trim();
					String result = null;
					if (ac.equals("base")) {
						result = base_CL(baseInfo, jsonObject);
						mos.write("base", new Text(result), new Text(""));
					} else if (ac.equals("loc")) {
						result = loc_CL(baseInfo, jsonObject);
						mos.write("loc", new Text(result), new Text(""));
					} else if (ac.equals("pc")) {
						result = pc_CL(baseInfo, jsonObject);
						mos.write("pc", new Text(result), new Text(""));
					} else if (ac.equals("apk_list")) {
						result = apk_list_CL(baseInfo, jsonObject);
						mos.write("apklist", new Text(result), new Text(""));
					} else if (ac.equals("apk_his")) {
						result = apk_his_CL(baseInfo, jsonObject);
						mos.write("apkhis", new Text(result), new Text(""));
					} else if (ac.equals("apk_use")) {
						result = apk_use_CL(baseInfo, jsonObject);
						mos.write("apkuse", new Text(result), new Text(""));
					} else if (ac.equals("apk_def")) {
						result = apk_def_CL(baseInfo, jsonObject);
						mos.write("apkdef", new Text(result), new Text(""));
					} else if (ac.equals("apk_f")) {
						result = apk_f_CL(baseInfo, jsonObject);
						mos.write("apkf", new Text(result), new Text(""));
					} else if (ac.equals("gps")) {
						result = gps_CL(baseInfo, jsonObject);
						mos.write("gps", new Text(result), new Text(""));
					} else if (ac.equals("blue")) {
						result = blue_CL(baseInfo, jsonObject);
						mos.write("blue", new Text(result), new Text(""));
					} else if (ac.equals("url")) {
						result = url_CL(baseInfo, jsonObject);
						mos.write("url", new Text(result), new Text(""));
					} else if (ac.equals("phonelst")) {
						result = phonelst_CL(baseInfo, jsonObject);
						mos.write("phonelst", new Text(result), new Text(""));
					} else if (ac.equals("schedule")) {
						result = schedule_CL(baseInfo, jsonObject);
						mos.write("schedule", new Text(result), new Text(""));
					} else if (ac.equals("sdcard")) {
						result = sdcard_CL(baseInfo, jsonObject);
						mos.write("sdcard", new Text(result), new Text(""));
					} else if (ac.equals("earphone")) {
						result = earphone_CL(baseInfo, jsonObject);
						mos.write("earphone", new Text(result), new Text(""));
					} else if (ac.equals("charging")) {
						result = charging_CL(baseInfo, jsonObject);
						mos.write("charging", new Text(result), new Text(""));
					} else if (ac.equals("cable")) {
						result = cable_CL(baseInfo, jsonObject);
						mos.write("cable", new Text(result), new Text(""));
					} else if (ac.equals("phonerec")) {
						result = phonerec_CL(baseInfo, jsonObject);
						mos.write("phonerec", new Text(result), new Text(""));
					} else if (ac.equals("photo_lst")) {
						result = photo_lst_CL(baseInfo, jsonObject);
						mos.write("photolst", new Text(result), new Text(""));
					} else if (ac.equals("message_con")) {
						result = message_con_CL(baseInfo, jsonObject);
						mos.write("messagecon", new Text(result), new Text(""));
					} else {

					}

				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.error(e);
					continue;
				}
			}
		}

		/**
		 * 17.数据线连接信息
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String cable_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String time = jsonObject.getString("time");
				String freq = jsonObject.getString("freq");

				result = String.format("%s\t%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, time, freq);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("cable_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 20.短信
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String message_con_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray mes_lst = jsonObject.getJSONArray("mes_lst");
				String arrayString = "";
				for (int i = 0; i < mes_lst.length(); i++) {

					JSONObject jsonObject2 = mes_lst.getJSONObject(i);
					String mes = jsonObject2.getString("mes");
					String structString = String.format("%s", mes);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("message_con_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 19.相册信息
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String photo_lst_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				int num = jsonObject.getInt("num");

				result = String.format("%s\t%s\t%s\t%s\t%d", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, num);
				JSONArray lbs = jsonObject.getJSONArray("lbs");
				String arrayString = "";
				for (int i = 0; i < lbs.length(); i++) {

					JSONObject jsonObject2 = lbs.getJSONObject(i);
					String addr = jsonObject2.getString("addr");
					String structString = String.format("%s", addr);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("photo_lst_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 18.通话记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String phonerec_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray phone_lst = jsonObject.getJSONArray("phone_lst");
				String arrayString = "";
				for (int i = 0; i < phone_lst.length(); i++) {

					JSONObject jsonObject2 = phone_lst.getJSONObject(i);
					String phoneNo = jsonObject2.getString("phoneNo");
					String time = jsonObject2.getString("time");
					String c_or_r = jsonObject2.getString("c_or_r");
					String time_t = jsonObject2.getString("time_t");
					String structString = String.format("%s:%s:%s:%s", phoneNo,
							time, c_or_r, time_t);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("phonerec_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 16.充电信息
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String charging_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String time = jsonObject.getString("time");
				String freq = jsonObject.getString("freq");

				result = String.format("%s\t%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, time, freq);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("charging_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 15.耳机相关
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String earphone_CL(String[] baseInfo, JSONObject jsonObject) {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String op = jsonObject.getString("op");
				String lbs = jsonObject.getString("lbs");

				result = String.format("%s\t%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, op, lbs);
			} catch (JSONException e) {
				log.error("earphone_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 14.sdcard多媒体文件信息
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String sdcard_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray lst = jsonObject.getJSONArray("lst");
				String arrayString = "";
				for (int i = 0; i < lst.length(); i++) {

					JSONObject jsonObject2 = lst.getJSONObject(i);
					String nm = jsonObject2.getString("nm");
					String lbl = jsonObject2.getString("lbl");
					String structString = String.format("%s:%s", nm, lbl);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("sdcard_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 13.日程和闹钟
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String schedule_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray lst = jsonObject.getJSONArray("lst");
				String arrayString = "";
				for (int i = 0; i < lst.length(); i++) {

					JSONObject jsonObject2 = lst.getJSONObject(i);
					String t = jsonObject2.getString("t");
					String f = jsonObject2.getString("f");
					String lbl = jsonObject2.getString("lbl");
					String on = jsonObject2.getString("on");
					String structString = String.format("%s:%s:%s:%s", t, f,
							lbl, on);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("schedule_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 12.通讯录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String phonelst_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray lst = jsonObject.getJSONArray("lst");
				String arrayString = "";
				for (int i = 0; i < lst.length(); i++) {

					JSONObject jsonObject2 = lst.getJSONObject(i);
					String num = jsonObject2.getString("num");
					String lbl = jsonObject2.getString("lbl");
					String structString = String.format("%s:%s", num, lbl);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("phonelst_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 11.url访问记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String url_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String n = jsonObject.getString("n");

				result = String.format("%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, n);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("url_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 10.蓝牙开关记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String blue_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String op = jsonObject.getString("op");

				result = String.format("%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, op);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("blue_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 9.gps开关记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String gps_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String op = jsonObject.getString("op");

				result = String.format("%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, op);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("gps_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 8.应用流量记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String apk_f_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}
				String dt_f = jsonObject.getString("dt_f");
				result = String.format("%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, dt_f);
				JSONArray apk_f = jsonObject.getJSONArray("apk_f");
				String arrayString = "";
				for (int i = 0; i < apk_f.length(); i++) {

					JSONObject jsonObject2 = apk_f.getJSONObject(i);
					String apk = jsonObject2.getString("apk");
					String n = jsonObject2.getString("n");
					String w = jsonObject2.getString("w");
					String g = jsonObject2.getString("g");
					String g3 = jsonObject2.getString("g3");
					String g4 = jsonObject2.getString("g4");
					String structString = String.format("%s:%s:%s:%s:%s:%s",
							apk, n, w, g, g3, g4);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("apk_f_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 7.默认应用
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String apk_def_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}
				// int h = jsonObject.getInt("h");
				// String lbs = jsonObject.getString("lbs");
				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray apk_def = jsonObject.getJSONArray("apk_def");
				String arrayString = "";
				for (int i = 0; i < apk_def.length(); i++) {

					JSONObject jsonObject2 = apk_def.getJSONObject(i);
					int tp = jsonObject2.getInt("tp");
					String apk = jsonObject2.getString("apk");
					String n = jsonObject2.getString("n");
					String v = jsonObject2.getString("v");
					String v_n = jsonObject2.getString("v_n");
					String structString = String.format("%d:%s:%s:%s:%s", tp,
							apk, n, v, v_n);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("apk_def_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 6.应用使用记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String apk_use_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}
				int h = jsonObject.getInt("h");
				String lbs = jsonObject.getString("lbs");
				result = String.format("%s\t%s\t%s\t%s\t%d\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt, h, lbs);
				JSONArray apk_use = jsonObject.getJSONArray("apk_use");
				String arrayString = "";
				for (int i = 0; i < apk_use.length(); i++) {

					JSONObject jsonObject2 = apk_use.getJSONObject(i);
					String apk = jsonObject2.getString("apk");
					String n = jsonObject2.getString("n");
					String v = jsonObject2.getString("v");
					String v_n = jsonObject2.getString("v_n");
					int c = jsonObject2.getInt("c");
					int t = jsonObject2.getInt("t");
					String structString = String.format("%s:%s:%s:%s:%d:%d",
							apk, n, v, v_n, c, t);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("apk_use_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 5.应用安装，卸载记录
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 */
		private String apk_his_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String op = jsonObject.getString("op");
				String apk = jsonObject.getString("apk");
				String n = jsonObject.getString("n");
				String v = jsonObject.getString("v");
				String v_n = jsonObject.getString("v_n");

				result = String.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
						baseInfo[0], baseInfo[1], baseInfo[2], dt, op, apk, n,
						v, v_n);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("apk_his_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 4.应用列表
		 * 
		 * @param baseInfo
		 * @param jsonObject
		 * @return
		 * @throws JSONException
		 */
		private String apk_list_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}
				result = String.format("%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], dt);
				JSONArray apk_list = jsonObject.getJSONArray("apk_list");
				String arrayString = "";
				for (int i = 0; i < apk_list.length(); i++) {

					JSONObject jsonObject2 = apk_list.getJSONObject(i);
					String apk = jsonObject2.getString("apk");
					String n = jsonObject2.getString("n");
					String v = jsonObject2.getString("v");
					String v_n = jsonObject2.getString("v_n");
					String structString = String.format("%s:%s:%s:%s", apk, n,
							v, v_n);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("apk_list_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 3.5.省市信息
		 * 
		 * @param values
		 * @param mos2
		 */
		private String pc_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String p = jsonObject.getString("p");
				String c = jsonObject.getString("c");
				result = String.format("%s\t%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], p, c, dt);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("pc_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 3.经纬度信息、省市信息
		 * 
		 * @param values
		 * @param mos2
		 */
		private String loc_CL(String[] baseInfo, JSONObject jsonObject) {
			// TODO Auto-generated method stub

			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}

				String la = jsonObject.getString("la");
				String lo = jsonObject.getString("lo");
				result = String.format("%s\t%s\t%s\t%s\t%s\t%s", baseInfo[0],
						baseInfo[1], baseInfo[2], la, lo, dt);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("loc_CL_err: " + e.getMessage());
				e.printStackTrace();
			}

			return result;
		}

		/**
		 * 2.手机基础信息
		 * 
		 * @param values
		 */
		private String base_CL(String[] baseInfo, JSONObject jsonObject)
				throws JSONException {
			String result = null;
			try {
				String dt = jsonObject.getString("dt");
				if (dt != null) {
					dt = stampToDate(dt);
				}
				String mac = jsonObject.getString("mac");
				String mf = jsonObject.getString("mf");
				String v = jsonObject.getString("v");
				String v_f = jsonObject.getString("v_f");
				String ram = jsonObject.getString("ram");
				String rom = jsonObject.getString("rom");
				String ch = jsonObject.getString("ch");
				String cst = jsonObject.getString("cst");
				String b = jsonObject.getString("b");
				String pro = jsonObject.getString("pro");
				String pt = jsonObject.getString("pt");
				String lcd = jsonObject.getString("lcd");
				String g = jsonObject.getString("g");
				String cpu = jsonObject.getString("cpu");
				String md = jsonObject.getString("md");
				String w = jsonObject.getString("w");
				String s = jsonObject.getString("s");
				String qq = jsonObject.getString("qq");

				result = String
						.format("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s",
								dt, baseInfo[0], baseInfo[1], baseInfo[2], mac,
								mf, v, v_f, ram, rom, ch, cst, b, pro, pt, lcd,
								g, cpu, md, w, s, qq);

				JSONArray sen = jsonObject.getJSONArray("sen");
				String arrayString = "";
				for (int i = 0; i < sen.length(); i++) {

					JSONObject jsonObject2 = sen.getJSONObject(i);
					String n = jsonObject2.getString("n");
					String tp = jsonObject2.getString("tp");
					String structString = String.format("%s:%s", n, tp);

					arrayString += structString + ",";
				}
				if (arrayString.endsWith(",")) {
					arrayString = arrayString.substring(0,
							arrayString.length() - 1);
				}

				result += "\t" + arrayString;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("base_CL_err: " + e.getMessage());
				e.printStackTrace();
			}
			return result;
		}

		/**
		 * 生成JSONObject
		 * 
		 * @param line
		 * @return
		 */
		private JSONObject jsonParse(String line) {
			// TODO Auto-generated method stub
			line = line.substring(line.indexOf("\t") + 1);
			int startIndex = line.indexOf("{");
			int stopIndex = line.lastIndexOf("}");
			if (startIndex == -1 || stopIndex == -1) {
				return null;
			}
			line = line.substring(startIndex, stopIndex + 1);
			JSONObject jsonObject = null;
			try {
				jsonObject = new JSONObject(line);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				log.error("jsonParse_err: " + e.getMessage());
				e.printStackTrace();
			}
			return jsonObject;
		}

		/**
		 * 基本信息处理 返回list[] = {imsi,imei,uuid}
		 * 
		 * @param line
		 * @return
		 */
		private String[] baseInfoCL(String line) {
			String[] baseInfo = null;
			try {
				baseInfo = line.split("\t")[0].split("_");
				if (baseInfo == null || baseInfo.length != 3) {
					log.error("baseInfo 转换错误");
					return null;
				}
			} catch (Exception e) {
				// TODO: handle exception
				log.error("baseInfoCL_err： " + e.getMessage());
				return null;
			}
			return baseInfo;
		}

		/*
		 * 将时间戳转换为时间
		 */
		public static String stampToDate(String s) {
			String res = null;
			try {
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
						"yyyy-MM-dd HH:mm:ss");
				long lt = new Long(s);
				Date date = new Date(lt);
				res = simpleDateFormat.format(date);
			} catch (Exception e) {
				// TODO: handle exception
				log.error("时间戳转换错误stampToDate_err： dt = " + s);
				return s;
			}
			return res;
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
			mos.close();
		}
	}
}
