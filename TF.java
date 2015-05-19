package ee6895ta;

import static ee6895ta.OCRObject.*;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.stat.interval.ConfidenceInterval;
import org.apache.commons.math3.stat.interval.NormalApproximationInterval;

import scala.Tuple2;

public class TF implements Comparable<TF>, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7738084869738437121L;

	public enum Rloc {
		IN("IN"), ABOVE("ABOVE"), BELOW("BELOW"), PATHED("PATH-DEPENDENT");
		private String name;

		private Rloc(String s) {
			name = s;
		}

		public String toString() {
			return name;
		}
	};

	public static NormalApproximationInterval nai = new NormalApproximationInterval();

	public final String token;
	public final int freq;

	public TF(String t, int f) {
		this.token = t;
		this.freq = f;
	}

	/*
	 * Returns whether the given token could be variant of this token given Ergo by being less than
	 * the max predicted frequency range.
	 * NOTE: an upgrade would scale the stdDev according to the number of edits,
	 * so that errors don't accumulate during repeated application of a given Conf Interval
	 */
	public boolean hasLeftVariant(TF v, Ergo ergo, double predictionZ) throws Exception {
		if (this.token.equals(v.token)) return true;
		if (v.freq >= this.freq) return false;

		return !location(v, ergo, predictionZ, false).equals(Rloc.ABOVE);
	}

	/*
	 * Joint log probability mass
	 */
	public double jlpm(TF v, Ergo ergo) throws Exception {

		verbose(v.token, "Examining " + "\n" + this.toString() + " -> " + v.toString() + "...");

		if (this.token.equals(v.token)) return 0;

		Edit[] edits = this.edits(v).toArray(new Edit[] {});
		double probs[] = new double[edits.length];

		for (int i = 0; i < edits.length; i++) {
			Edit edit = edits[i];
			probs[i] = ergo.postProb(edit.from(), edit.to(), (edits.length == 1 ? v.freq : 0));
		}

		double jp = 0;
		for (int i = 0; i < edits.length; i++) {
			jp += Math.log(probs[i]);
		}
		return jp;
	}
	
	/*
	 * Location of the given variant, NOTE: more than two edits only traverses one path and
	 * therefore is not recommended
	 */
	public Rloc location(TF v, Ergo ergo, double predictionZ, boolean emit) throws Exception {

		if (this.token.equals(v.token)) return Rloc.IN;
		if (v.freq >= this.freq) return Rloc.ABOVE;

		int totalTrials = this.freq + v.freq;
		Range predictRange = new Range(totalTrials, totalTrials);
		verbose(v.token, "\n" + this.toString() + " -> " + v.toString() + " tt=" + totalTrials);

		Edit[] edits = this.edits(v).toArray(new Edit[] {});
		double probs[] = new double[edits.length];

		for (int i = 0; i < edits.length; i++) {
			Edit edit = edits[i];
			probs[i] = ergo.postProb(edit.from(), edit.to(), (edits.length == 1 ? v.freq : 0));
		}

		// 3-edits will create a problem - don't search them.
		if (edits.length == 2 && (probs[0] < probs[1])) {
			Edit he = edits[0];
			edits[0] = edits[1];
			edits[1] = he;
			double hp = probs[0];
			probs[0] = probs[1];
			probs[1] = hp;
		}

		for (int i = 0; i < edits.length; i++) {
			predictRange.setMin(predictOccurrences(predictRange.getMin(), probs[i], predictionZ).getMin());
			predictRange.setMax(predictOccurrences(predictRange.getMax(), probs[i], predictionZ).getMax());
			verbose(emit, "Edit " + edits[i].toString() + " predicts range " + predictRange + " using prob = " + probs[i]);
		}

		Rloc rloc = (v.freq < predictRange.getMin() ? Rloc.BELOW : (v.freq > predictRange.getMax() ? Rloc.ABOVE : Rloc.IN));
		verbose(emit, " is " + rloc);
		return rloc;
	}

	/*
	 * Return the predicted frequency range of occurences of the given edit for the given number of
	 * tokens
	 */
	public Range predictOccurrences(int trials, double prob, double z) {
		if (trials < 2) return new Range(0, 2);
		if (prob <= 0) return new Range(0, 0);
		BinomialDistribution dist = new BinomialDistribution(trials, prob);
		double stdDev = Math.sqrt(dist.getNumericalVariance());
		return new Range(dist.getNumericalMean() - z * stdDev, dist.getNumericalMean() + z * stdDev);
	}
	
	/*
	 * Probability of the given number of occurrences
	 */
	public double probOcc(int trials, double prob, int occ) {
		BinomialDistribution dist = new BinomialDistribution(trials, prob);
		return Math.log(dist.probability(occ));
	}

	/*
	 * Confidence interval for a universal error rate misread
	 */
	public ConfidenceInterval probMisreadAs(TF v) {
		if (this.token.equals(v.token)) return null;
		if (v.freq >= this.freq) return new ConfidenceInterval(DEFACTO_ONE - EPSILON, DEFACTO_ONE + EPSILON, DEFACTO_ONE);
		int edits = this.edits(v).size();
		int trials = this.freq * edits + v.freq * edits;
		int succs = v.freq * edits;
		return nai.createInterval(trials, succs, CONFIDENCE_LEVEL);
	}

	/*
	 * Edits
	 */
	public ArrayList<Edit> edits(TF v) {
		assert (this.token.length() == v.token.length());
		ArrayList<Edit> edits = new ArrayList<Edit>();
		char[] tcs = this.token.toCharArray();
		char[] vcs = v.token.toCharArray();
		for (int i = 0; i < tcs.length; i++) {
			if (tcs[i] != vcs[i]) {
				edits.add(new Edit(tcs[i], vcs[i]));
			}
		}
		return edits;
	}

	/*
	 * The 1-* DVF that separates this TF from the given TF
	 */
	public ArrayList<DVF> dvf(TF v) {
		ArrayList<DVF> dvfs = new ArrayList<DVF>();
		ArrayList<Edit> edits = this.edits(v);
		if (edits.size() == 1) {
			dvfs.add(new DVF(edits.get(0).from() + edits.get(0).to(), v.freq));
		}
		return dvfs;
	}

	@Override
	/*
	 * Always sorted in DESC by default
	 */
	public int compareTo(TF o) {
		if (o == null) return 1;
		return o.freq > this.freq ? 1 : (this.freq != o.freq ? -1 : 0);
	}

	public Tuple2<String, Integer> t2() {
		return new Tuple2<String, Integer>(token, freq);
	}

	public String literal() {
		return token + COMMA + freq;
	}

	@Override
	public String toString() {
		return this.token + " (" + this.freq + ")";
	}

}
