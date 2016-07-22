package main;

import org.apache.spark.api.java.*;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;

import scala.Tuple2;

public class Reporter {

	private static JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD;

	private static String output_path;

	public static void main(String[] args) {

		if (args.length < 1) {
			System.out.println("Expected 1 arguments: output_path");
			System.exit(1);
		}
		output_path = args[0];

		SparkConf conf = new SparkConf().setAppName("BugzillaReporter");
		JavaSparkContext sc = new JavaSparkContext(conf);

		Configuration hbc = HBaseConfiguration.create();
		hbc.set(TableInputFormat.INPUT_TABLE, Analyzer.TABLE_NAME);

		hbaseRDD = sc.newAPIHadoopRDD(hbc, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

		generateReport();
	}

	private static void generateReport() {
		try {
			Path pt = new Path(output_path + "/report.html");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(fs.create(pt, true)));
			br.write(Report.generateHTML(tot_bugs(), tot_datasets(), bugs_fixed(), bugs_closed(), bugs_open(),
					bugs_invalid(), security_bugs(), io_bugs(), memory_bugs(), network_bugs(), bugs_per_function(),
					bugs_per_year(), bugs_per_hour(), bugs_per_program(), bugs_per_component(), bugs_languages(),
					italian_users(), users_tot_bugs(), users_tot_datasets()));
			br.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static long tot_datasets() {
		return hbaseRDD.count();
	}

	private static long getSum(String column, String qualifier) {
		return hbaseRDD.map(t -> Bytes.toLong(t._2().getValue(Bytes.toBytes(column), Bytes.toBytes(qualifier))))
				.reduce((x, y) -> x + y);
	}

	private static long tot_bugs() {
		return getSum("general", "tot_bugs");
	}

	private static long bugs_fixed() {
		return getSum("status", "bugs_fixed");
	}

	private static long bugs_closed() {
		return getSum("status", "bugs_closed");
	}

	private static long bugs_open() {
		return getSum("status", "bugs_open");
	}

	private static long bugs_invalid() {
		return getSum("status", "bugs_invalid");
	}

	private static long security_bugs() {
		return getSum("type", "security_bugs");
	}

	private static long io_bugs() {
		return getSum("type", "io_bugs");
	}

	private static long memory_bugs() {
		return getSum("type", "memory_bugs");
	}

	private static long network_bugs() {
		return getSum("type", "network_bugs");
	}

	private static long italian_users() {
		return getSum("users", "italian_users");
	}

	private static List<Tuple2<String, Long>> getMapCountSumSortedByValue(String column, String qualifier) {
		return hbaseRDD.flatMapToPair(t -> {
			List<Tuple2<String, Long>> ret = new LinkedList<Tuple2<String, Long>>();
			Map<String, Long> curr = byte2map(t._2().getValue(Bytes.toBytes(column), Bytes.toBytes(qualifier)));
			for (Map.Entry<String, Long> entry : curr.entrySet())
				ret.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
			return ret;
		}).reduceByKey((x, y) -> x + y).mapToPair(t -> new Tuple2<Long, String>(t._2(), t._1())).sortByKey(false)
				.mapToPair(t -> new Tuple2<String, Long>(t._2(), t._1())).collect();
	}

	private static List<Tuple2<String, Long>> getMapCountSumSortedByKey(String column, String qualifier) {
		return hbaseRDD.flatMapToPair(t -> {
			List<Tuple2<String, Long>> ret = new LinkedList<Tuple2<String, Long>>();
			Map<String, Long> curr = byte2map(t._2().getValue(Bytes.toBytes(column), Bytes.toBytes(qualifier)));
			for (Map.Entry<String, Long> entry : curr.entrySet())
				ret.add(new Tuple2<String, Long>(entry.getKey(), entry.getValue()));
			return ret;
		}).reduceByKey((x, y) -> x + y).sortByKey(true).collect();
	}

	private static List<Tuple2<String, Long>> bugs_per_function() {
		return getMapCountSumSortedByValue("code", "bugs_per_function");
	}

	private static List<Tuple2<String, Long>> bugs_per_year() {
		return getMapCountSumSortedByKey("time", "bugs_per_year");
	}

	private static List<Tuple2<String, Long>> bugs_per_hour() {
		return getMapCountSumSortedByKey("time", "bugs_per_hour");
	}

	private static List<Tuple2<String, Long>> bugs_per_program() {
		return getMapCountSumSortedByValue("code", "bugs_per_program");
	}

	private static List<Tuple2<String, Long>> bugs_per_component() {
		return getMapCountSumSortedByValue("code", "bugs_per_component");
	}

	private static List<Tuple2<String, Long>> bugs_languages() {
		return getMapCountSumSortedByValue("languages", "bugs_languages");
	}

	private static List<Tuple2<String, Long>> users_tot_bugs() {
		return getMapCountSumSortedByValue("users", "bugs_per_user");
	}

	private static List<Tuple2<String, Long>> users_tot_datasets() {
		return hbaseRDD.flatMapToPair(t -> {
			List<Tuple2<String, Long>> ret = new LinkedList<Tuple2<String, Long>>();
			Map<String, Long> curr = byte2map(t._2().getValue(Bytes.toBytes("users"), Bytes.toBytes("bugs_per_user")));
			for (Map.Entry<String, Long> entry : curr.entrySet())
				ret.add(new Tuple2<String, Long>(entry.getKey(), 1L));
			return ret;
		}).reduceByKey((x, y) -> x + y).mapToPair(t -> new Tuple2<Long, String>(t._2(), t._1())).sortByKey(false)
				.mapToPair(t -> new Tuple2<String, Long>(t._2(), t._1())).collect();
	}

	private static <K, V> Map<K, V> byte2map(byte[] value) {
		ByteArrayInputStream byteIn = new ByteArrayInputStream(value);
		ObjectInputStream in;
		try {
			in = new ObjectInputStream(byteIn);
			return (Map<K, V>) in.readObject();
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

}
