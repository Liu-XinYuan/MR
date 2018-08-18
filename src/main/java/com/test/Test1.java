package com.test;

import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.jdo.identity.IntIdentity;

import com.sun.tools.internal.xjc.reader.xmlschema.bindinfo.BIConversion.Static;

public class Test1 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Test01();
		// Test02();
		// Test03();
		// Test04();

		// Test05();
		// Test06();
		// Test07();

		Test08();

	}

	public static void Test08() {
		String path = "hdfs://nameservice1/tmp/sxm/dir01/SafeData-2016-07-12.har/part-0";
		String el = "\\d{4}-[0,1][0-9]-[0-3][0-9].har";
		Pattern pattern = Pattern.compile(el);
		Matcher matcher = pattern.matcher(path);
		if (matcher.find()) {
			String aim = matcher.group();
			String dt = aim.substring(0,aim.length()-4);
			System.out.println(dt);
		}

	}

	public static void Test07() {
		String str = "as\tdd\nss\r\nd";
		System.out.println(str);
		str = str.replace("\t", "/t/").replace("\r\n", "/r/n/")
				.replace("\n", "/n/");
		System.out.println(str);
	}

	public static void Test06() {
		String str = "__ ";
		String[] sp = str.split("_");
		for (int i = 0; i < sp.length; i++) {
			System.out.println(String.format("i = %d value = %s", i, sp[i]));
		}
		System.out.println(sp.length);
	}

	public static void Test05() {
		String oldString = "\uD800";
		String newString = null;
		try {
			newString = new String(oldString.getBytes("UTF-8"), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out
				.println(String
						.format("oldString = %s\nnewString = %s\nnewString.equals(oldString) = %b",
								oldString, newString,
								newString.equals(oldString)));
	}

	public static void Test04() {
		String str = "asdfdff90()sdafffPASREvgfdasrOAfthoisadf-5r289567834ASDFUYUASGFASgasdfpaasdf+_-=`21iqwure237462534572906872tyupqhfsmxoiuxmcv,by.l oi  eeGaBNJK<>?";
		int[] chars = new int[128];
		// 初始化赋值0
		for (int i = 0; i < chars.length; i++) {
			chars[i] = 0;
		}
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			chars[c] += 1;
		}
		for (int i = 0; i < chars.length; i++) {
			if (chars[i] != 0) {
				System.out.println(String.format("字符%c出现的次数为：%d", i, chars[i]));
			}
		}
	}

	public static void Test03() {
		String line = "aaa{bbbbccc{fh}";
		int startIndex = line.indexOf("{");
		int stopIndex = line.lastIndexOf("}");
		System.out.println(String.format("startIndex = %s\t stopIndex = %s",
				startIndex, stopIndex));
		if (startIndex == -1 || stopIndex == -1) {
			System.err.println("error");
			return;
		}
		line = line.substring(startIndex, stopIndex + 1);
		System.out.println(line);
	}

	public static void Test01() {
		double d = Math.max(2.5, Math.min(2.8, 2.6));
		System.out.println(d);
	}

	public static void Test02() {
		// int a = -2;
		// String a = "2";

		// double a = 0, b = 0, c;
		// c = a / (a + b);
		// if (c != c) {
		// System.out.println("c!=c");
		// } else {
		// System.err.println("c!=c");
		// }

		double a = 0 / 0.0;
		double b = 1 / 0.0;
		System.out.println(a);
		System.out.println(a == a);
		System.out.println("-----------------------------");
		System.out.println(b);
		System.out.println(b == b);
	}
}
