package ee6895ta;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public final class InitRawVariants extends OCRUtility implements Serializable {

	public InitRawVariants(int tokenLength) {
		super(tokenLength);
	}

	/*
	 * ---------------------------------------------------------------------------
	 * ---------------------------------------------------------------------------
	 */
	public List<DVF> rawVariants(String inputFile, final Ergo ergo, int tokenLength) throws Exception {

		this.TOKEN_LENGTH = tokenLength;
		verbose("\n--------------------------\n   " + tokenLength + " Variant Frequencies \n--------------------------");
			
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

		verbose("  generating 1-* pattern classes...");
		PairFlatMapFunction<TF, String, TF> flatMapToPcTf = new PairFlatMapFunction<TF, String, TF>() {
			public Iterable<Tuple2<String, TF>> call(TF tf) throws Exception {
				ArrayList<Tuple2<String, TF>> pctfs = new ArrayList<Tuple2<String, TF>>();
				char[] tchars = tf.token.toCharArray();
				for (int i = 0; i < tchars.length; i++) {
					char keep = tchars[i];
					tchars[i] = STAR;
					pctfs.add(new Tuple2<String, TF>(new String(tchars), tf));
					tchars[i] = keep;
				}
				return pctfs;
			}
		};
		JavaPairRDD<String, Iterable<TF>> pctfs = tfs.flatMapToPair(flatMapToPcTf).groupByKey();
		verbose("  ... pattern classes generated.");

		verbose("  generating variants from each pattern class...");

		PairFlatMapFunction<Tuple2<String, Iterable<TF>>, String, Integer> flatMapToDvifs = new PairFlatMapFunction<Tuple2<String, Iterable<TF>>, String, Integer>() {
			public Iterable<Tuple2<String, Integer>> call(Tuple2<String, Iterable<TF>> pctfs) throws Exception {
				ArrayList<Tuple2<String, Integer>> dvfs = new ArrayList<Tuple2<String, Integer>>();
				int starpos = pctfs._1.indexOf(STAR);
				if (starpos < 0) throw new RuntimeException("No star in PC:" + pctfs._1);
				ArrayList<TF> tfs = new ArrayList<TF>();
				Iterator<TF> tfiter = pctfs._2.iterator();
				while (tfiter.hasNext()) {
					tfs.add(tfiter.next());
				}
				// TFs in desc freq
				Collections.sort(tfs);
				for (int i = 1; i < tfs.size(); i++) {
					for (int h = 0; h < i; h++) {
						char[] dvc = new char[2];
						dvc[0] = tfs.get(h).token.charAt(starpos);
						dvc[1] = tfs.get(i).token.charAt(starpos);
						dvfs.add(new Tuple2<String, Integer>(new String(dvc), tfs.get(i).freq));
					}
				}
				return dvfs;
			}
		};

		Function<Tuple2<String, Integer>, DVF> mapToDVF = new Function<Tuple2<String, Integer>, DVF>() {
			public DVF call(Tuple2<String, Integer> t) throws Exception {
				return new DVF(t._1, t._2);
			}	
		};
		JavaRDD<DVF> dvfs = pctfs.flatMapToPair(flatMapToDvifs).reduceByKey(counterFunction).map(mapToDVF);
		List<DVF> list_dvfs = dvfs.collect();
		verbose("  ...  variants generated, aggregated.");

		ctx.stop();
		ctx.close();
		return list_dvfs;
	}

}