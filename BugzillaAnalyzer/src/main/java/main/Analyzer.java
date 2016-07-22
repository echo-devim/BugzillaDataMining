package main;

import org.apache.spark.api.java.*;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class Analyzer {

	final static public String TABLE_NAME = "bugzilla_results";
	static public String languages_path;
	
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Expected 2 arguments: dataset_path languages_path");
			System.exit(1);
		}

		languages_path = args[1];
		
		String rowkey = args[0].substring(args[0].lastIndexOf('/')+1, args[0].length()-4);
		String folder = args[0].substring(0, args[0].lastIndexOf("/"));

		SparkConf conf = new SparkConf().setAppName("BugzillaAnalyzer");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> bugs = sc.textFile(args[0]).cache();
		System.out.println("Path names: "+folder+"/names.txt");
		JavaRDD<String> names = sc.textFile(folder+"/names.txt").cache();



		Configuration hbc = HBaseConfiguration.create();
		hbc.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

		Job newAPIJobConfiguration1 = null;
		try {
			newAPIJobConfiguration1 = Job.getInstance(hbc);
			newAPIJobConfiguration1.setOutputFormatClass(org.apache.hadoop.hbase.mapreduce.TableOutputFormat.class);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		putSingleValue(hbc, rowkey, "general", "tot_bugs", Bytes.toBytes(tot_bugs(bugs)));
		long bugs_fixed = bugs_fixed(bugs);
		putSingleValue(hbc, rowkey, "status", "bugs_fixed", Bytes.toBytes(bugs_fixed));
		putSingleValue(hbc, rowkey, "code", "bugs_per_function", map2byte(bugs_per_function(bugs)));
		putSingleValue(hbc, rowkey, "time", "bugs_per_year", map2byte(bugs_per_year(bugs)));
		putSingleValue(hbc, rowkey, "time", "bugs_per_hour", map2byte(bugs_per_hour(bugs)));
		putSingleValue(hbc, rowkey, "code", "bugs_per_program", map2byte(bugs_per_program(bugs)));
		putSingleValue(hbc, rowkey, "code", "bugs_per_component", map2byte(bugs_per_component(bugs)));
		putSingleValue(hbc, rowkey, "status", "bugs_open", Bytes.toBytes(bugs_open(bugs)));
		long bugs_closed = bugs_closed(bugs);
		putSingleValue(hbc, rowkey, "status", "bugs_closed", Bytes.toBytes(bugs_closed));
		putSingleValue(hbc, rowkey, "status", "bugs_invalid", Bytes.toBytes(bugs_closed - bugs_fixed));
		putSingleValue(hbc, rowkey, "type", "memory_bugs", Bytes.toBytes(memory_bugs(bugs)));
		putSingleValue(hbc, rowkey, "type", "security_bugs", Bytes.toBytes(security_bugs(bugs)));
		putSingleValue(hbc, rowkey, "type", "io_bugs", Bytes.toBytes(io_bugs(bugs)));
		putSingleValue(hbc, rowkey, "type", "network_bugs", Bytes.toBytes(network_bugs(bugs)));
		putSingleValue(hbc, rowkey, "users", "italian_users", Bytes.toBytes(italian_users(bugs, names)));
		putSingleValue(hbc, rowkey, "languages", "bugs_languages", map2byte(bugs_languages(bugs)));
		putSingleValue(hbc, rowkey, "users", "bugs_per_user", map2byte(users(bugs)));
		//putSingleValue(hbc, rowkey, "rels", "programs", map2byte(programs(bugs)));
	}

	private static void putSingleValue(Configuration conf, String rowkey, String column, String qualifier, byte[] value) {
		
		if (value == null) {
			System.err.println("Received NULL value");
			return;
		}
		
		try {
			HTable hTable = new HTable(conf, TABLE_NAME);
			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(Bytes.toBytes(column), Bytes.toBytes(qualifier), value);
			hTable.put(put);
			hTable.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static long tot_bugs (JavaRDD<String> bugs) {
		return bugs.count();
	}


	private static Map<String,Long> bugs_per_year(JavaRDD<String> bugs) {
		return bugs.map( l-> {
			String[] t = l.split(",\"");
			return t[t.length-1].substring(0, t[t.length-1].indexOf("-"));
		}).countByValue();
	}

	private static Map<String, Long> bugs_per_hour(JavaRDD<String> bugs) {
		return bugs.map( l-> {
			String[] t = l.split(",\"");
			String hour = t[t.length-1].split(" ")[1];
			return hour.substring(0, hour.indexOf(":"));
		}).countByValue();
	}

	private static Map<String, Long> bugs_per_function(JavaRDD<String> bugs) {

		return bugs.map(l -> {
			String[] t = l.split("\",\"");
			if(t.length < 6)
				return "";
			else
				return t[5];
		}).filter(s -> !s.equals("")).map(s -> s.toLowerCase()).flatMap( s -> {
			List<String> funcs = new ArrayList<String>();
			Pattern pattern = Pattern.compile("([a-z_][a-z0-9]*_[a-z0-9_]*_[a-z0-9^\\.]*|[a-z0-9_]+ ?\\(\\))");
			Matcher matcher = pattern.matcher(s);
			while (matcher.find())
			{
				funcs.add(matcher.group());
			}
			return funcs;
		}).map(f -> f.replace("(","").replace(")","") ).countByValue();

	}

	private static Map<String, Long> bugs_per_program(JavaRDD<String> bugs) {
		return bugs.map( l -> l.split(",\"")[1]).map(l -> l.substring(0,l.length()-1)).map(s -> s.toLowerCase()).countByValue();
	}

	private static Map<String, Long> bugs_per_component(JavaRDD<String> bugs) {
		return bugs.map( l -> l.split("\",\"")[1]).map(s -> s.toLowerCase()).countByValue();
	}

	private static long bugs_fixed (JavaRDD<String> bugs) {
		return bugs.filter(l -> l.split(",\"")[5].equals("FIXED\"") ).count();
	}

	private static long bugs_open (JavaRDD<String> bugs) { 

		return bugs.filter(l -> {
			String s = l.split(",\"")[4];
			return s.equals("UNCONFIRMED\"") || s.equals("ASSIGNED\"") || s.equals("NEW\"") || s.equals("REOPENED\"") || s.equals("NEEDINFO\"") || s.equals("NEEDSINFO\""); 
		}).count();

	}

	private static long bugs_closed (JavaRDD<String> bugs) {
		return bugs.filter(l -> {
			String s = l.split(",\"")[4];
			return s.equals("CLOSED\"") || s.equals("RESOLVED\""); 
		}).count();
	}

	private static long security_bugs (JavaRDD<String> bugs) {
		String[] keywords = {"security","secure","insecure","unsecure","secured","non-secure","vulnerable","vulnerability","overflow","overflows","unauthorized","unautheticated","injection","format string","use-after-free","use after free"};
		return bugs.map(l -> {
			String[] t = l.split("\",\"");
			if(t.length < 6)
				return "";
			else
				return t[5];
		}).filter(s -> !s.equals("")).map(s -> s.toLowerCase()).filter( c -> {
			for (String keyword : keywords) {
				if (c.contains(keyword)) return true;
			}
			return false;
		}).count();
	}

	private static long memory_bugs (JavaRDD<String> bugs) {
		String[] keywords = {"memory","heap","stack","invalid pointer","sigsegv","address 0x","segmentation", "double free"};
		return bugs.map(l -> {
			String[] t = l.split("\",\"");
			if(t.length < 6)
				return "";
			else
				return t[5];
		}).filter(s -> !s.equals("")).map(s -> s.toLowerCase()).filter( c -> {
			for (String keyword : keywords) {
				if (c.contains(keyword)) return true;
			}
			return false;
		}).count();
	}

	private static long io_bugs (JavaRDD<String> bugs) {
		String[] keywords = {"i/o","disk","hdd","ssd"};
		return bugs.map(l -> {
			String[] t = l.split("\",\"");
			if(t.length < 6)
				return "";
			else
				return t[5];
		}).filter(s -> !s.equals("")).map(s -> s.toLowerCase()).filter( c -> {
			for (String keyword : keywords) {
				if (c.contains(keyword)) return true;
			}
			return false;
		}).count();
	}

	private static long network_bugs (JavaRDD<String> bugs) {
		String[] keywords = {"network","internet"};
		return bugs.map(l -> {
			String[] t = l.split("\",\"");
			if(t.length < 6)
				return "";
			else
				return t[5];
		}).filter(s -> !s.equals("")).map(s -> s.toLowerCase()).filter( c -> {
			for (String keyword : keywords) {
				if (c.contains(keyword)) return true;
			}
			return false;
		}).count();
	}

	private static long italian_users (JavaRDD<String> bugs, JavaRDD<String> names) {
		return bugs.map(l -> l.split(",")[3]).distinct()
				.flatMapToPair( n -> {
					List<Tuple2<String,Long>> list = new ArrayList<>();
					String[] p = n.split("[^a-z]");
					for (String s : p) list.add(new Tuple2<>(s,(long) p.hashCode() & 0xffffffffl)); //int to uint
					return list;
				}).distinct().filter(t -> t._2() < 0).union( names.mapToPair(n -> new Tuple2<String,Long> (n.toLowerCase(),-1l)) )
				.reduceByKey( (v1,v2) -> v1*v2 ).filter(t -> t._2() < 0).mapToPair(t -> new Tuple2<Long,String> (t._2(), t._1()))
				.reduceByKey((s1,s2) -> s1).count();

	}

	private static Map<String, Long> bugs_languages (JavaRDD<String> bugs) {
		try {
			DetectorFactory.loadProfile(languages_path);    
		} catch (LangDetectException e) {
			System.out.println("Language detection error: "+e.getMessage());
			System.out.println("Languages path: " + languages_path);
		}
		return bugs.map(l -> {
			String[] t = l.split("\",\"");
			if(t.length < 6)
				return "";
			else
				return t[5];
		}).filter( s -> !s.equals("")).map(c -> c.toLowerCase()).map(c -> {
			Detector detector = DetectorFactory.create();
			detector.append(c);
			String key = "";
			try {
				key = detector.detect();
			} catch (LangDetectException e) {
				System.err.println("LangDetectException: "+e.getMessage());
				return "";
			}
			return key;
		}).filter(s -> !s.equals("")).countByValue();
	}

	private static Map<String, Long> users (JavaRDD<String> bugs) {
		return bugs.map( l -> l.split("\",\"")[2]).countByValue();
	}
	
	private static Map<String, List<String>> programs (JavaSparkContext sc, JavaRDD<String> bugs) {
		return bugs.mapToPair( l -> {
			String[] s = l.split(",\"");
			if (s.length < 8)
				return new Tuple2<String,String>(s[1],"");
			else
				return new Tuple2<String,String>(s[1],s[6]);
		}).mapToPair(t -> {
			return new Tuple2<String,List<String>>(t._1(),Arrays.asList(t._2().split(" ")));
		}).reduceByKey( (r1,r2) -> { List<String> t = new ArrayList<String>(r1); t.addAll(r2); return r1; } ).mapToPair(t -> {
			return new Tuple2<String, List<String>>(t._1(), t._2().parallelStream().map(s -> s.toLowerCase()).filter(s -> s.length() > 3).distinct().collect(Collectors.toList()));
		}).collectAsMap();
	}

	private static <K, V> byte[] map2byte(Map<K,V> map){
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		try {
			ObjectOutputStream out = new ObjectOutputStream(byteOut);
			out.writeObject(map);
			return byteOut.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

	}


}