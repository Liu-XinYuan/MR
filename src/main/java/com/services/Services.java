package com.services;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

public class Services {

	// public static void main(String[] args) {
	//
	// String gz = "D:/desktop2/tmp2/gtTest/SafeData-2016-07-12.har.tar.gz";
	// String result = "D:/desktop2/tmp2/gtTest/result";
	// if (doUncompressFile(gz, result) != 0) {
	// System.err.println("gz 解压错误");
	// }
	//
	// System.out.println("gz 解压完成");
	//
	// String tar = "D:/desktop2/tmp2/gtTest/SafeData-2016-07-12.har.tar";
	// if (!extTarFileList(tar, result)) {
	// System.err.println("tar 解压错误");
	// }
	//
	// System.out.println("tar 解压完成");
	//
	// System.exit(0);
	// }

	/**
	 * 解压tar包
	 * 
	 * @author: kpchen@iflyte.com
	 * @createTime: 2015年9月23日 下午5:41:56
	 * @history:
	 * @param filename
	 *            tar文件
	 * @param directory
	 *            解压目录
	 * @return
	 */
	public static boolean extTarFileList(String filename, String directory,
			Logger log) {
		boolean flag = false;
		OutputStream out = null;
		try {

			if (directory == null) {
				directory = getParentDir(filename);
			}
			directory = dirCL(directory);
			log.info("directory = " + directory);
			TarInputStream in = new TarInputStream(new FileInputStream(
					new File(filename)));
			TarEntry entry = null;
			while ((entry = in.getNextEntry()) != null) {
				if (entry.isDirectory()) {
					continue;
				}
				log.info(entry.getName());

				File outfile = new File(directory + entry.getName());
				new File(outfile.getParent()).mkdirs();
				out = new BufferedOutputStream(new FileOutputStream(outfile));
				int x = 0;
				while ((x = in.read()) != -1) {
					out.write(x);
				}
				out.close();
			}
			in.close();
			flag = true;
		} catch (IOException ioe) {
			ioe.printStackTrace();
			flag = false;
			log.error("IOException: " + ioe.getMessage());
		}
		return flag;
	}

	/**
	 * Perform file compression.
	 * 
	 * @param inFileName
	 *            Name of the file to be compressed
	 */
	public static int doCompressFile(String inFileName) {

		try {

			String outFileName = inFileName + ".gz";
			GZIPOutputStream out = null;
			try {
				out = new GZIPOutputStream(new FileOutputStream(outFileName));
			} catch (FileNotFoundException e) {
				return -1;
			}

			FileInputStream in = null;
			try {
				in = new FileInputStream(inFileName);
			} catch (FileNotFoundException e) {
				return -1;
			}

			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}
			in.close();

			out.finish();
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
			return -1;
		}
		return 0;
	}

	/**
	 * 
	 * @param inFileName
	 * @param outputDir
	 *            :文件解压目录，为空则为压缩文件当前目录
	 * @return
	 */
	public static int doUncompressFile(String inFileName, String outputDir,
			Logger log) {

		try {

			if (!getExtension(inFileName).equalsIgnoreCase("gz")) {
				log.error("!getExtension(inFileName).equalsIgnoreCase(gz)");
				return -1;
			}

			GZIPInputStream in = null;
			try {
				in = new GZIPInputStream(new FileInputStream(inFileName));
			} catch (FileNotFoundException e) {
				log.error("FileNotFoundException: " + e.getMessage());
				return -1;
			}

			String outFileName = null;
			// 如果outputDir为空，则默认解药到当前文件夹
			if (outputDir != null) {
				outFileName = outputDir + "/" + getFileName(inFileName);
			} else {
				outFileName = getALLFileName(inFileName);
			}
			FileOutputStream out = null;
			try {
				out = new FileOutputStream(outFileName);
			} catch (FileNotFoundException e) {
				log.error("FileNotFoundException: " + e.getMessage());
				return -1;
			}

			byte[] buf = new byte[1024];
			int len;
			while ((len = in.read(buf)) > 0) {
				out.write(buf, 0, len);
			}

			in.close();
			out.close();

		} catch (IOException e) {
			e.printStackTrace();
			log.error("IOException: " + e.getMessage());
			return -1;
		}
		return 0;
	}

	public static String getExtension(String f) {
		String ext = "";
		int i = f.lastIndexOf('.');

		if (i > 0 && i < f.length() - 1) {
			ext = f.substring(i + 1);
		}
		return ext;
	}

	public static String getALLFileName(String f) {
		String fname = "";
		int i = f.lastIndexOf('.');

		if (i > 0 && i < f.length() - 1) {
			fname = f.substring(0, i);
		}
		return fname;
	}

	public static String getFileName(String f) {
		String fname = "";
		int i = f.lastIndexOf("/");
		int k = f.lastIndexOf(".");
		if (k > 0 && k < (f.length() - 1) && i > 0 && i < k) {
			fname = f.substring(i + 1, k);
		}
		return fname;
	}

	public static String getParentDir(String f) {
		// TODO Auto-generated method stub
		String parentDir = "";

		int i = f.lastIndexOf("/");
		if (i > 0) {
			parentDir = f.substring(0, i + 1);
		}
		return parentDir;
	}

	public static String dirCL(String dir) {
		if (!dir.endsWith("/")) {
			dir = dir + "/";
		}
		return dir;
	}

}
