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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

@SuppressWarnings("serial")
public final class IdentifyMisreads extends OCRUtility implements Serializable {

	public static final double Z_ADMITTANCE = 3.0;

	public IdentifyMisreads() {
		super();
	}

	public List<Misread> mireadsOf(final TF truef, String inputFile, final Ergo ergo, int maxMisreads) throws Exception {

		this.TOKEN_LENGTH = truef.token.length();
		if (!ergo.isInference(truef)) return new ArrayList<Misread>();
		
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
		
		/*
		 * Generate all lower frequency uninferred variants of the dominant
		 */
		verbose("Generating lower frequency uninferred variants...");
		final HashSet<String> dompats = addAllPatterns(truef, SEARCH_RADIUS, null);
		FlatMapFunction<Tuple2<String, Iterable<TF>>, TF> flatMapToVariants = new FlatMapFunction<Tuple2<String, Iterable<TF>>, TF>() {
			public Iterable<TF> call(Tuple2<String, Iterable<TF>> pc_tfs) throws Exception {
				ArrayList<TF> tfs = new ArrayList<TF>();
				if (!dompats.contains(pc_tfs._1)) return tfs;
				
				Iterator<TF> iterator = pc_tfs._2.iterator();
				while (iterator.hasNext()) {
					TF vf = iterator.next();
					if (truef.freq > vf.freq && !ergo.isInference(vf)) tfs.add(vf);
				}
				return tfs;
			}		
		};
		List<TF> variants = pctfs.flatMap(flatMapToVariants).distinct().collect();
		int lowestFreqVariant = Integer.MAX_VALUE;
		for (TF vf : variants) {
			if (vf.freq < lowestFreqVariant) lowestFreqVariant = vf.freq;
		}		
		verbose("... generated " + variants.size() + " variants.");
		
		/*
		 * the pattern classes of all reachables
		 */
		HashSet<String> variantPatternClasses = new HashSet<String>();
		for (TF vf : variants) {	
				addAllPatterns(vf, SEARCH_RADIUS, variantPatternClasses);
		}
		
		verbose("Generating all reachable considerations...");
		final HashSet<String> rpcs = variantPatternClasses;
		final int reachableFloor = lowestFreqVariant;
		FlatMapFunction<Tuple2<String, Iterable<TF>>, TF> flatMapToReachables = new FlatMapFunction<Tuple2<String, Iterable<TF>>, TF>() {
			public Iterable<TF> call(Tuple2<String, Iterable<TF>> pc_tfs) throws Exception {
				ArrayList<TF> tfs = new ArrayList<TF>();
				if (!rpcs.contains(pc_tfs._1)) return tfs;
				for (TF hfdf : pc_tfs._2) {
					if (hfdf.freq > reachableFloor) tfs.add(hfdf);
				}
				return tfs;
			}
		};
		List<TF> reachables = pctfs.flatMap(flatMapToReachables).distinct().collect();
		verbose("... generated " + reachables.size() + " reachables.");
		
		ArrayList<Misread> misreads = new ArrayList<Misread>();
		
		//msg("Reachables:\n" + reachables);
		Collections.sort(reachables);
		int domix = -1;
		for (int i=0; i<reachables.size();i++) {
			if (reachables.get(i).token.equals(truef.token)) {
				domix = i;
				break;
			}
		}
		
		if (domix < 0) {
			msg("Not self-reachable!");
			domix = reachables.size();
		}
		
		verbose("Considering reachables...");
		for (int i=domix+1; i<reachables.size(); i++) {
			TF c = reachables.get(i);
			if (ergo.isInference(c)) continue;
			if (truef.edits(c).size() > SEARCH_RADIUS) continue;
			if (!truef.hasLeftVariant(c, ergo, Z_ADMITTANCE)) continue;
			
			TF mlcf = reachables.get(0);
			double hiLogProb = reachables.get(0).jlpm(c, ergo);
			
			for (int h=1; h<i; h++) {
				TF hftf = reachables.get(h);
				if (c.edits(hftf).size() > SEARCH_RADIUS) continue;
				if (hftf.jlpm(c, ergo) > hiLogProb) {
					mlcf = hftf;
					hiLogProb = hftf.jlpm(c, ergo);
				}
			}
			
			if (mlcf.token.equals(truef.token)) {
				verbose("   identified " + c.toString() + " ... ");
				misreads.add(new Misread(c, hiLogProb));
				if (misreads.size() >= maxMisreads) break;
			}
		}
		
		Collections.sort(misreads);
		
		
		ctx.stop();
		ctx.close();
		
		verbose("Misreads:\n" + misreads);
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