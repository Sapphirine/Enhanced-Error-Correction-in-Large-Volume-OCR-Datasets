package ee6895ta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public final class InitRawFrequencies extends OCRUtility implements Serializable {

	
	public List<TF> list_tfs; 
	public ArrayList<TF> tops;
	List<Tuple2<String, Integer>> list_charfreqs ;
	
	/*
	 * ---------------------------------------------------------------------------
	 * Constructor
	 * ---------------------------------------------------------------------------
	 */
	public InitRawFrequencies() {
		super(0);
	}	
	
	public InitRawFrequencies(int tokenLength) {
		super(tokenLength);
	}	
	
	/*
	 * ---------------------------------------------------------------------------
	 * Initialize all characters
	 * ---------------------------------------------------------------------------
	 */
	public  void collectRawFrequencies(String inputFile, final Ergo ergo, int tokenLength) throws Exception {

		this.TOKEN_LENGTH = tokenLength;
		
		verbose("\n----------------------\n   " + tokenLength + " Token Frequencies \n----------------------");
		SparkConf sparkConf = new SparkConf().setAppName("OCRUtility");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inputFile, 1);

		Function<String, TF> mapTFliteralKeytoTF = new Function<String, TF>() {
			public TF call(String s) throws Exception {
				String[] tfla = s.split(COMMA);
				return new TF(tfla[0], Integer.parseInt(tfla[1]));
			}
		};
		JavaRDD<TF> tfs = lines.filter(metaFilter).map(mapTFliteralKeytoTF).filter(makeTFLengthFilter(tokenLength));
		verbose("RDD<TF> of token lengths = " + tokenLength + " created.");
		
		list_tfs = tfs.collect();
		verbose("Collecting top " + CACHE_TOP_LEN + "...");
		Collections.sort(list_tfs);
		tops = new ArrayList<TF>();
		for (int i=0; (i< list_tfs.size() && i<=CACHE_TOP_LEN); i++) {
			tops.add(list_tfs.get(i));
		}
		verbose("... collected.");
		
		PairFlatMapFunction<TF, String, Integer> flatMapToCharFreqs = new PairFlatMapFunction<TF, String, Integer>() {
			public Iterable<Tuple2<String, Integer>> call(TF tf) throws Exception {
				ArrayList<Tuple2<String, Integer>> cfs = new ArrayList<Tuple2<String, Integer>>();
				for (int i=0; i<tf.token.length();i++) {
					cfs.add(new Tuple2<String, Integer>( tf.token.substring(i, i+1), tf.freq));
				}
				return cfs;
			}
		};
		verbose("Counting raw character frequencies...");
		JavaPairRDD<String, Integer> charFreqs = tfs.flatMapToPair(flatMapToCharFreqs).reduceByKey(counterFunction);
		verbose("... count completed.");
		
		list_charfreqs = charFreqs.collect();
		
		ctx.stop();
		ctx.close();
		
	}
	
	/*
	 * ---------------------------------------------------------------------------
	 * Initialize all characters
	 * ---------------------------------------------------------------------------
	 */
	public  void initializeChars(String inputFile, final Ergo ergo) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("OCRUtility");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inputFile, 1);

		Function<String, TF> mapTFliteralKeytoTF = new Function<String, TF>() {
			public TF call(String s) throws Exception {
				String[] tfla = s.split(COMMA);
				return new TF(tfla[0], Integer.parseInt(tfla[1]));
			}
		};
		JavaRDD<TF> tfs = lines.filter(metaFilter).map(mapTFliteralKeytoTF).filter(minMaxLengthFilter);
		verbose("Input file read, mapped and filtered to RDD<TF>");
		
		//   FLAT MAP:  side effect function to load hash set
		final HashSet<String> chars = new HashSet<String>();
		FlatMapFunction<TF, String> extractEachChar = new FlatMapFunction<TF, String>() {
			public Iterable<String> call(TF tf) throws Exception {
				ArrayList<String> cs = new ArrayList<String>();
				for (int i=0; i<tf.token.length();i++) {
					String newchar = tf.token.substring(i, i+1);
					if (chars.add(newchar)) {
						cs.add(newchar);
					}
				}
				return cs;
			}
		};
	
		verbose("Collecting distinct characters...");
		JavaRDD<String> distinctChars = tfs.flatMap(extractEachChar).distinct();
		List<String> dchars = distinctChars.collect();
		verbose("... " + dchars.size() + " distinct characters collected.");
		
		ctx.stop();
		ctx.close();
		
		ergo.initialize(dchars);
	}

}