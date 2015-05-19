package ee6895ta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public final class InferTruth extends OCRUtility implements Serializable {

	public static final double Z_ADMITTANCE = 3.0;
	
	public InferTruth(int tokenLength) {
		super(tokenLength);
	}

	public List<Tuple2<String, Iterable<ArrayList<DVF>>>> inferences(String inputFile, final Ergo ergo, int tokenLength) throws Exception {

		this.TOKEN_LENGTH = tokenLength;

		verbose("\n----------------------\n   " + tokenLength + " Token Inferences \n----------------------");
		
		SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inputFile, 1);

		Function<String, TF> mapTFliteralKeytoTF = new Function<String, TF>() {
			public TF call(String s) throws Exception {
				String[] tfla = s.split(COMMA);
				return new TF(tfla[0], Integer.parseInt(tfla[1]));
			}
		};
		JavaRDD<TF> tfs = lines.filter(metaFilter).map(mapTFliteralKeytoTF).filter(makeTFLengthFilter(tokenLength));
		verbose(tokenLength + "-char token RDD<TF> created...");

		verbose("  generating all pattern classes...");
		final ArrayList<HashSet<Integer>> indexSets = makeIndexSet(TOKEN_LENGTH, ERROR_RADIUS);
		PairFlatMapFunction<TF, String, TF> flatMapToPcTf = new PairFlatMapFunction<TF, String, TF>() {
			public Iterable<Tuple2<String, TF>> call(TF tf) throws Exception {
				ArrayList<Tuple2<String, TF>> pctfs = new ArrayList<Tuple2<String, TF>>();
				for (String pc : patternize(tf.token, indexSets)) {
					pctfs.add(new Tuple2<String, TF>(pc, tf));
				}
				return pctfs;
			}
		};
		JavaPairRDD<String, Iterable<TF>> pctfs = tfs.flatMapToPair(flatMapToPcTf).groupByKey();
		verbose("  ... pattern classes generated.");

		verbose("  collecting non-admitted variants from each pattern class...");

		PairFlatMapFunction<Tuple2<String, Iterable<TF>>, String, ArrayList<DVF>> generateKickouts = new PairFlatMapFunction<Tuple2<String, Iterable<TF>>, String, ArrayList<DVF>>() {
			public Iterable<Tuple2<String, ArrayList<DVF>>> call(Tuple2<String, Iterable<TF>> pc_tfs) throws Exception {
				ArrayList<Tuple2<String, ArrayList<DVF>>> t_dvfs = new ArrayList<Tuple2<String, ArrayList<DVF>>>();

				ArrayList<TF> tfs = new ArrayList<TF>();
				Iterator<TF> tfiter = pc_tfs._2.iterator();
				while (tfiter.hasNext()) {
					tfs.add(tfiter.next());
				}
				// TFs in desc freq
				Collections.sort(tfs);

				for (int i = 0; i < tfs.size(); i++) { // each tf
					
					if (tfs.get(i).freq < MIN_SUPPORT_FOR_INFERENCE || ergo.isInference(tfs.get(i))) continue;
					
					if (i == 0) {
						t_dvfs.add(new Tuple2<String, ArrayList<DVF>>(tfs.get(0).token, new ArrayList<DVF>()));
						continue;
					}
					
					ArrayList<DVF> dvfs = new ArrayList<DVF>();
					TF vtf = tfs.get(i);
					
					for (int h = 0; h < i; h++) { // each higher freq tf
						if (!tfs.get(h).hasLeftVariant(tfs.get(i), ergo, Z_ADMITTANCE)) {
							TF htf = tfs.get(h);
							dvfs.addAll(htf.dvf(vtf));
						}
					} // next hf-tf
					
					t_dvfs.add(new Tuple2<String, ArrayList<DVF>>(vtf.token, dvfs));
				} // next tf

				return t_dvfs;
			}
		};
		
		JavaPairRDD<String, ArrayList<DVF>> kickouts = pctfs.flatMapToPair(generateKickouts);
		kickouts.cache();
		
		tempDir = "kickouts";
		clearSequenceDirectory(tempDir);
		kickouts.saveAsTextFile(tempDir);
		
		final int totalClassCount = totalPatternClassCount(TOKEN_LENGTH, ERROR_RADIUS);
		
		final List<String> its = kickouts.mapToPair(new PairFunction<Tuple2<String, ArrayList<DVF>>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple2<String, ArrayList<DVF>> t) throws Exception {
				return new Tuple2<String, Integer>(t._1, 1);
			}
		}).reduceByKey(counterFunction).filter(new Function<Tuple2<String,Integer>, Boolean>() {
			public Boolean call(Tuple2<String, Integer> t) throws Exception {
				return t._2 == totalClassCount;
			}
		}).keys().collect();
		
		JavaPairRDD<String, Iterable<ArrayList<DVF>>> inferences = kickouts.filter(new Function<Tuple2<String,ArrayList<DVF>>, Boolean>() {
			public Boolean call(Tuple2<String, ArrayList<DVF>> t) throws Exception {
				return its.contains(t._1);
			}
		}).groupByKey();
				
		 tempDir = "inferences";
		clearSequenceDirectory(tempDir);
		inferences.saveAsTextFile(tempDir);

		verbose("   collected non-admitted variants.");
		
		List<Tuple2<String, Iterable<ArrayList<DVF>>>> list_inferences = inferences.collect();

		ctx.stop();
		ctx.close();
		
		return list_inferences;

	}

}