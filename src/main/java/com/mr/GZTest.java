package com.mr;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;

import com.services.Services;

public class GZTest {
	private static final Logger log = Logger.getLogger(GZTest.class.getName());

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub

		String inputPath = args[1];
		String flag = args[2];
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		List<Path> paths = getAllPath(fs, new Path(inputPath));
		for (Path path : paths) {
			String info = String.format("class = %s\nname = %s\nparent = %s",
					path.getClass(), path.getName(), path.getParent());
			log.info("info ： " + info);

			if (flag.equals("1")) {
				uncompress1(path.toString());
			} else if (flag.equals("2")) {
				untar(path.toString());
			} else if (flag.equals("1")) {
				uncompress1(path.toString());
				untar(path.toString());
			}

		}

		System.exit(0);
	}

	static public List<Path> getAllPath(FileSystem fs, Path path) {
		List<Path> pathList = new ArrayList<Path>();

		try {
			if (fs.isFile(path)) {
				// if (path.toString().endsWith(".gz")) {
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
					List<Path> list = getAllPath(fs, item_path);
					if (list != null && list.size() > 0) {
						pathList.addAll(list);
					}
				} else if (fs.isFile(item_path)) {
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

	public static void untar(String uri) {
		Services.extTarFileList(uri, null, log);
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
}
