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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public final class Corrector extends OCRUtility implements Serializable {

	public static final double Z_ADMITTANCE = 3.0;
	public static final double MAX_NEG_LOG_PROB = -20.0;

	public Corrector() {
		super();
	}

	public List<Misread> correction(final TF error, String inputFile, final Ergo ergo) throws Exception {

		this.TOKEN_LENGTH = error.token.length();
		new ArrayList<Misread>();
		
		SparkConf sparkConf = new SparkConf().setAppName(this.getClass().getSimpleName());
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile(inputFile, 1);

		/*
		 * For closures, generate all the SEARCH_RADIUS pattern classes
		 */
		Function<String, TF> mapTFliteralKeytoTF = new Function<String, TF>() {
			public TF call(String s) throws Exception {
				String[] tfla = s.split(COMMA);
				return new TF(tfla[0], Integer.parseInt(tfla[1]));
			}
		};
		JavaRDD<TF> tfs = lines.filter(metaFilter).map(mapTFliteralKeytoTF).filter(makeTFLengthFilter(this.TOKEN_LENGTH));
		List<TF> alltfs = tfs.collect();
		
		final ArrayList<HashSet<Integer>> indexSets = makeIndexSet(TOKEN_LENGTH, SEARCH_RADIUS);
		PairFlatMapFunction<TF, String, TF> flatMapToPcTf = new PairFlatMapFunction<TF, String, TF>() {
			public Iterable<Tuple2<String, TF>> call(TF tf) throws Exception {
				ArrayList<Tuple2<String, TF>> pctfs = new ArrayList<Tuple2<String, TF>>();
				for (String pc : patternize(tf.token, indexSets)) {
					pctfs.add(new Tuple2<String, TF>(pc, tf));
				}
				return pctfs;
			}
		};
		verbose("Generating pattern classes...");
		JavaPairRDD<String, Iterable<TF>> pctfs = tfs.flatMapToPair(flatMapToPcTf).groupByKey();
		verbose("... generated " + pctfs.count() + " pattern classes.");
		
		ArrayList<Misread> misreads = new ArrayList<Misread>();
		
		Collections.sort(alltfs);
		
		verbose("Considering corrections...");
		for (int i=1; i<alltfs.size(); i++) {
			TF c = alltfs.get(i);
			if (!c.token.equals(error.token)) continue;			
	
			TF mlcf = null;
			double hiLogProb = MAX_NEG_LOG_PROB;
			
			for (int h=1; h<i; h++) {
				TF hftf = alltfs.get(h);
				if (!ergo.isInference(hftf)) continue;
				if (c.edits(hftf).size() > SEARCH_RADIUS) continue;
				
				if (hftf.jlpm(c, ergo) > hiLogProb) {
					mlcf = hftf;
					hiLogProb = hftf.jlpm(c, ergo);
				}
			}
			
			if (mlcf == null) continue;
			if (mlcf.hasLeftVariant(c, ergo, Z_ADMITTANCE)) {
				misreads.add(new Misread(c, hiLogProb));
				verbose("  identified " + c.toString()  + " as misread of " + mlcf.toString());
				if (misreads.size() >= 1) break;
			}
	
		}
		
		Collections.sort(misreads);
		
		
		ctx.stop();
		ctx.close();
		
		verbose("Corrections:\n" + misreads);
		return misreads;

	}

	public HashSet<String> addAllPatterns(TF tf, int radius, HashSet<String> pats) {
		if (pats == null) pats = new HashSet<String>();
		ArrayList<HashSet<Integer>> indexSets = makeIndexSet(tf.token.length(), radius);
		for (String pc : patternize(tf.token, indexSets)) {
			pats.add(pc);
		}
		return pats;
	}

}