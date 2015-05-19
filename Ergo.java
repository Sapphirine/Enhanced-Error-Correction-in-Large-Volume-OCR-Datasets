package ee6895ta;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import scala.Tuple2;
import static ee6895ta.OCRUtility.*;

@SuppressWarnings("serial")
public class Ergo implements Serializable {

	public static final String SER_DIRECTORY = "ergo";

	public enum Inference {
		GROUND, EXTENSION, NEIGHBOR, INFER
	};

	private static final String SER_TOPTFS = "/toptfs.ser";
	private static final String SER_RFMAP = "/rfmap.ser";
	private static final String SER_DVMAP = "/dvmap.ser";
	private static final String SER_INF = "/infer.ser";
	private static final String SER_TFMAP = "/tfmap.ser";

	private ArrayList<TF> topTFs;
	private HashMap<String, Integer> tfmap;
	private HashMap<String, Integer> rfmap;
	private HashMap<String, HashMap<String, Integer>> dvmap;

	private HashMap<String, Inference> inferences = new HashMap<String, Inference>(10000);

	/*
	 * ---------------------------------------------------------------------------
	 * Constructor
	 * ---------------------------------------------------------------------------
	 */
	public Ergo() {
		super();
	}

	/*
	 * Initialize the tables with the given strings
	 */
	public void initialize(List<String> allchars) {
		if (rfmap != null) throw new RuntimeException("Initialization has already occurred.");

		rfmap = new HashMap<String, Integer>();
		dvmap = new HashMap<String, HashMap<String, Integer>>();

		for (String rc : allchars) {
			if (rc.equals(STAR_STRING) || rc.equals(COMMA)) continue;

			rfmap.put(rc, 0);

			// Create all cells for this one row, each rc is also a dom char in the dv map
			HashMap<String, Integer> drowCell = new HashMap<String, Integer>();
			for (String v : allchars) {
				drowCell.put(v, 0);
			}
			dvmap.put(rc, drowCell);
		}
	}

	/*
	 * Add
	 */
	public int cacheTokens(List<TF> tfs) {
		if (tfmap==null) tfmap = new HashMap<String, Integer>(tfs.size());
		for (TF tf : tfs) {
			tfmap.put(tf.token, tf.freq);
		}
		return tfs.size();
	}

	/*
	 * Cache top TFs
	 */
	public int cacheTop(List<TF> tfs) {
		if (topTFs == null) topTFs = new ArrayList<TF>();
		for (TF tf : tfs) {
			topTFs.add(tf);
		}
		return tfs.size();
	}

	public int freq(String t) {
		Integer f = tfmap.get(t);
		return (f == null ? 0 : f);
	}

	/*
	 * Capture raw char frequencies
	 */
	public void rawCharOccurrence(String rt, int rf) {
		for (int i = 0; i < rt.length(); i++) {
			String key = rt.substring(i, i + 1);
			rfmap.put(key, rfmap.get(key) + rf);
		}
	}

	/*
	 * Declare all these inferences
	 */
	public void declareInferences(List<Tuple2<String, Iterable<ArrayList<DVF>>>> list_token_dvfs) {
		verbose("  Declaring " + list_token_dvfs.size() + " inferences to Ergo...");
		int toti = 0;
		for (Tuple2<String, Iterable<ArrayList<DVF>>> token_dvfs : list_token_dvfs) {

			if (!infer(token_dvfs._1, Inference.INFER)) return;
			toti++;

			// Array list may duplicate entries, keep track
			HashSet<String> reductions = new HashSet<String>();

			Iterator<ArrayList<DVF>> iter_dvfs = token_dvfs._2.iterator();
			while (iter_dvfs.hasNext()) {
				for (DVF dvf : iter_dvfs.next()) {
					if (reductions.contains(dvf.token)) continue;
					removeVariant(dvf);
					reductions.add(dvf.token);
				}
			}
		}
		verbose(" ... Ergo accepted " + toti + " inferences.");
	}

	/*
	 * Declare an occurrence of the given variation
	 */
	public void declareVariants(List<DVF> dvfs) {
		for (DVF dvf : dvfs) {
			declareVariant(dvf);
		}
		verbose("Ergo updated with " + dvfs.size() + " variants.");
	}

	/*
	 * Declare an occurrence of the given variation
	 */
	public void declareVariant(DVF dvf) {
		dvmap.get(dvf.d()).put(dvf.v(), dvmap.get(dvf.d()).get(dvf.v()) + dvf.freq);
	}

	/*
	 * Reduce the frequency of the given variation
	 */
	public void removeVariant(DVF dvf) {
		int newFreq = dvmap.get(dvf.d()).get(dvf.v()) - dvf.freq;
		if (newFreq < 0) {
			throw new RuntimeException("Removing variant " + dvf.toString() + " creates a negative frequency:");
		}
		dvmap.get(dvf.d()).put(dvf.v(), newFreq);
		verbose(dvf.d(), "Removed variant: " + dvf.toString());
	}

	/*
	 * Announce an inference, returning if it was accepted (that is, new)
	 */
	public boolean infer(String token, Inference inference) {
		if (inferences.containsKey(token)) return false;

		inferences.put(token, inference);
		verbose(token, "added inference " + token);

		return true;
	}

	/*
	 * Return max observable error rate
	 */
	public Tuple2<String, Double> maxErrorRate() throws Exception {
		Tuple2<String, Double> max = new Tuple2<String, Double>("NONE", 0.0);
		for (String key : rfmap.keySet()) {
			HashMap<String, Integer> trow = dvmap.get(key);
			for (String vkey : trow.keySet()) {
				if (key.equals(vkey)) continue;
				double thisProb = priorProb(key, vkey);
				if (thisProb > max._2) {
					max = new Tuple2<String, Double>(key + " -> " + vkey, thisProb);
				}
			}
		}
		return max;
	}

	/*
	 * Probability of the given variant
	 */
	public double priorProb(String t, String v) throws Exception {
		return postProb(t, v, 0);
	}

	/*
	 * Probability of the given variant if the given number of observations were declared
	 * non-variant (inferred)
	 */
	public double postProb(String t, String v, int obs) throws Exception {
		if (t.equals(v)) return DEFACTO_ONE;
		double tfreq = (double) rfmap.get(t);
		double vfreq = (double) dvmap.get(t).get(v) - obs;
		if (vfreq > tfreq) throw new Exception("Ergo frequency corruption:  truthChar =" + t + " and variant=" + v);
		if (vfreq < 0) throw new Exception("Obs=" + obs + " creates negative variant frequency:  truthChar =" + t + " and variant=" + v);
		return (tfreq == 0) ? 0 : vfreq / tfreq;
	}

	/*
	 * Return all top TFs of the given length
	 */
	public ArrayList<TF> topTFs(int length) {
		ArrayList<TF> tfs = new ArrayList<TF>();
		for (TF gtf : topTFs) {
			if (gtf.token.length() == length) tfs.add(gtf);
		}
		return tfs;
	}

	/*
	 * Sorted probs of the given char
	 */
	public ArrayList<VP> variantProbs(String d) throws Exception {
		ArrayList<VP> probs = new ArrayList<VP>();

		HashMap<String, Integer> drow = dvmap.get(d);
		ArrayList<String> vkeys = new ArrayList<String>(drow.keySet());
		for (String vkey : vkeys) {
			if (d.equals(vkey)) continue;
			probs.add(new VP(d, vkey, priorProb(d, vkey)));
		}

		Collections.sort(probs);
		return probs;
	}

	/*
	 * Lookup token freq NOT EFFICIENT
	 */
	public TF lookup(String t) {
		return new TF(t, freq(t));
	}

	boolean isInference(TF tf) {
		return isInference(tf.token);
	}

	boolean isInference(String t) {
		return inferences.containsKey(t);
	}

	/*
	 * toString
	 */
	public String output() throws Exception {
		return output(false);
	}

	/*
	 * toString
	 */
	public String output(boolean suppressVariants) throws Exception {
		return output(suppressVariants, 100);
	}

	/*
	 * toString
	 */
	public String output(boolean suppressVariants, int maxlist) throws Exception {

		String s = "\n----------------------------------------------------------\n";
		s += "    E R G O    \n";
		s += "----------------------------------------------------------\n";
		s += "Raw character frequencies:\n";
		ArrayList<String> keys = new ArrayList<String>(rfmap.keySet());
		Collections.sort(keys);
		for (String key : keys) {
			int freq = rfmap.get(key);
			s += key + "  " + freq + "\n";
		}

		if (!suppressVariants) {
			s += "\nVariant Frequencies:\n";
			for (String key : keys) {
				HashMap<String, Integer> trow = dvmap.get(key);
				ArrayList<String> vkeys = new ArrayList<String>(trow.keySet());
				Collections.sort(vkeys);
				for (String vkey : vkeys) {
					int freq = trow.get(vkey);
					s += key + " -> " + vkey + "     " + freq + "    " + priorProb(key, vkey) + "\n";
				}
			}
		}

		s += "\nInferred tokens:\n";
		Set<String> infers = inferences.keySet();
		if (infers.size() == 0) s += "  (None inferred)\n";
		ArrayList<String> its = new ArrayList<String>(infers);
		Collections.sort(its);
		int lines = 0;
		for (String it : its) {
			s += (it == null ? "NULL IS PRESENT IN THE SET" : it) + "\n";
			if (++lines > maxlist) {
				s += "\n  " + (its.size() - maxlist) + " more...";
				break;
			}
		}

		s += "\nMax Variant Probability: " + maxErrorRate().toString() + "\n";
		s += "Version " + this.version() + "\n";
		s += "\n----------------------------------------------------------\n";
		return s;

	}

	/*
	 * Save data to a directory
	 */
	public void saveTo(String dirname) throws Exception {
		verbose("Serializing ergo...");
		File directory = new File(dirname);
		if (!directory.exists()) {
			directory.mkdir();
		}
		serialize(new File(dirname + SER_RFMAP), rfmap);
		serialize(new File(dirname + SER_DVMAP), dvmap);
		serialize(new File(dirname + SER_TOPTFS), topTFs);
		serialize(new File(dirname + SER_INF), inferences);
		serialize(new File(dirname + SER_TFMAP), tfmap);
		serialize(new File(dirname + SER_TOPTFS), topTFs);
		verbose("... ergo serialization completed.");
	}

	/*
	 * Restore from a directory
	 */
	@SuppressWarnings("unchecked")
	public static Ergo restoreFrom(String dirname) throws Exception {
		msg("deserializing Ergo...");
		File gtFile = new File(dirname + SER_RFMAP);
		if (!gtFile.exists()) throw new RuntimeException("Non-extant file: " + gtFile.getCanonicalPath());

		Ergo e = new Ergo();
		e.rfmap = (HashMap<String, Integer>) deserialize(gtFile);
		e.dvmap = (HashMap<String, HashMap<String, Integer>>) deserialize(new File(dirname + SER_DVMAP));
		e.inferences = (HashMap<String, Inference>) deserialize(new File(dirname + SER_INF));
		e.tfmap = (HashMap<String, Integer>) deserialize(new File(dirname + SER_TFMAP));
		e.topTFs = (ArrayList<TF>) deserialize(new File(dirname + SER_TOPTFS));

		if (e.topTFs == null) msg("NULL toptf");
		if (e.tfmap == null) msg("null tfmap");

		msg("...deserialization completed.");
		return e;
	}

	/*
	 * private serializer
	 */
	private void serialize(File serFile, Object obj) throws Exception {
		if (serFile.exists()) serFile.delete();
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(serFile));
			oos.writeObject(obj);
		} finally {
			if (oos != null) oos.close();
		}
	}

	/*
	 * private deserializer
	 */
	private static Object deserialize(File serFile) throws Exception {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(serFile));
			return ois.readObject();
		} finally {
			if (ois != null) ois.close();
		}
	}

	/*
	 * Version number
	 */
	public String version() {
		return "I-" + (inferences == null ? "NULL" : inferences.size()) + " A-" + (tfmap == null ? "NULL" : tfmap.size()) + " T-"
				+ (topTFs == null ? "NULL" : topTFs.size());
	}

}
