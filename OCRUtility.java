package ee6895ta;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

@SuppressWarnings("serial")
public class OCRUtility extends OCRObject {


	
	/*
	 * ==============================================
	 * Instance variables
	 * ==============================================
	 */	
	public int TOKEN_LENGTH;
	public String tempDir;

	/*
	 * ----------------------------------------------------------------------------------------------
	 * 
	 * F U N C T I O N S
	 * 
	 * ----------------------------------------------------------------------------------------------
	 */
	// FUNCTION: counter
	Function2<Integer, Integer, Integer> counterFunction = new Function2<Integer, Integer, Integer>() {
		public Integer call(Integer i1, Integer i2) {
			return i1 + i2;
		}
	};

	/*
	 * ----------------------------------------------------------------------------------------------
	 * 
	 * F I L T E R S
	 * 
	 * ----------------------------------------------------------------------------------------------
	 */

	// FILTER: length
	Function<String, Boolean> makeLengthFilter(int len) {
		final int token_length = len;
		return new Function<String, Boolean>() {
			public Boolean call(String s) {
				return s.split(COMMA)[0].length() == token_length;
			}
		};
	}
	
	// FILTER: length
	Function<TF, Boolean> makeTFLengthFilter(int len) {
		final int token_length = len;
		return new Function<TF, Boolean>() {
			public Boolean call(TF tf) {
				return tf.token.length() == token_length;
			}
		};
	}
	
	// FILTER: freq
	Function<TF, Boolean> makeTFFreqFilter(TF tf) {
		final int minFreq = tf.freq;
		return new Function<TF, Boolean>() {
			public Boolean call(TF tf) {
				return tf.freq > minFreq;
			}
		};
	}
	
	// FILTER: filter token in this range
	Function<TF, Boolean> minMaxLengthFilter = new Function<TF, Boolean>() {
		public Boolean call(TF tf) {
			return tf.token.length() >= MIN_TOKEN_LENGTH && tf.token.length() <= MAX_TOKEN_LENGTH;
		}
	};	
	

	// FILTER: exclude meta chars
	Function<String, Boolean> metaFilter = new Function<String, Boolean>() {
		public Boolean call(String s) {
			return !s.contains(STAR_STRING) && !s.contains(EQUALS);
		}
	};

	public OCRUtility() {
		this(0);
	}

	public OCRUtility(int tokenLength) {
		this.TOKEN_LENGTH = tokenLength;
	}

	/*
	 * ---------------------------------------------------------------------------
	 * Generate all patterns for the given token
	 * ---------------------------------------------------------------------------
	 */
	protected ArrayList<String> patternize(String s, ArrayList<HashSet<Integer>> ixs) {
		ArrayList<String> pats = new ArrayList<String>();
		for (HashSet<Integer> iset : ixs) {
			char[] schars = s.toCharArray();
			for (int ix : iset) {
				schars[ix] = STAR;
			}
			pats.add(new String(schars));
		}
		return pats;
	}

	/*
	 * ---------------------------------------------------------------------------
	 * Binomial coeff util, n choose k
	 * ---------------------------------------------------------------------------
	 */
	public int binomCoeff(int n, int k) {
		if (k > n) {
			if (TOKEN_LENGTH == 0) return 0;
			throw new RuntimeException("k>n in binomCoeff");
		}
		int bc = fact(n) / (fact(k) * fact(n - k));
		return bc;
	}

	
	public int totalPatternClassCount(int tokenLen, int radius) {

		/*
		 * total number of pattern classes that could be dominated = add up the n-choose-k
		 * coefficients between 1 up to including the error radius
		 */
		int pclassCount = 0;
		for (int k = 1; k <= radius; k++) {
			pclassCount += binomCoeff(tokenLen, k);
		}
		return pclassCount;
		
	}
	
	/*
	 * ---------------------------------------------------------------------------
	 * Factorial
	 * ---------------------------------------------------------------------------
	 */
	public int fact(int x) {
		int f = 1;
		for (int i = 2; i <= x; i++) {
			f *= i;
		}
		return f;
	}

	/*
	 * ---------------------------------------------------------------------------
	 * Index sets
	 * ---------------------------------------------------------------------------
	 */
	protected ArrayList<HashSet<Integer>> makeIndexSet(int tokenLen, int radius) {
		ArrayList<HashSet<Integer>> indexSets = new ArrayList<HashSet<Integer>>();
		HashSet<Integer> nullset = new HashSet<Integer>();
		indexSets.add(nullset);
		fillIndexSets(indexSets, 1, tokenLen, radius);
		indexSets.remove(0);
		return indexSets;
	}

	/*
	 * ---------------------------------------------------------------------------
	 * Fill th index sets
	 * ---------------------------------------------------------------------------
	 */
	protected void fillIndexSets(ArrayList<HashSet<Integer>> indexSets, int cursize, int tokenLen, int radius) {
		if (cursize > radius) return;
		ArrayList<HashSet<Integer>> newsets = new ArrayList<HashSet<Integer>>();

		for (HashSet<Integer> is : indexSets) {
			if (is.size() != cursize - 1) {
				continue;
			}

			int maxIndex = -1;
			for (int ix : is) {
				maxIndex = Math.max(maxIndex, ix);
			}

			for (int i = maxIndex + 1; i < tokenLen; i++) {
				if (!is.contains(i)) {
					HashSet<Integer> newis = new HashSet<Integer>();
					newis.addAll(is);
					newis.add(i);
					newsets.add(newis);
				}
			}
		}
		indexSets.addAll(newsets);
		newsets.clear();
		fillIndexSets(indexSets, cursize + 1, tokenLen, radius);
	}

	/*
	 * ---------------------------------------------------------------------------
	 * From the given index sets,
	 * return a new set containing only those of a given size
	 * ---------------------------------------------------------------------------
	 */
	public ArrayList<HashSet<Integer>> starCountIndexSet(int starcount, ArrayList<HashSet<Integer>> ixsets) {
		ArrayList<HashSet<Integer>> starset = new ArrayList<HashSet<Integer>>();
		for (HashSet<Integer> is : ixsets) {
			if (is.size() == starcount) starset.add(is);
		}
		return starset;
	}

	/*
	 * -------------------------------------
	 * Print the index set
	 * --------------------------------------
	 */
	public void printIndexSet(ArrayList<HashSet<Integer>> indexSets) {
		msg("Index Set:");
		for (HashSet<Integer> iset : indexSets) {
			msg(iset.toString());
		}
	}

	/*
	 * --------------------------------
	 * Clear sequence directory
	 * --------------------------------
	 */
	public static void clearSequenceDirectory(String seqDirectory) throws Exception {

		delete(new File(seqDirectory + "/_SUCCESS"));
		delete(new File(seqDirectory + "/part-00000"));
		delete(new File(seqDirectory + "/._SUCCESS.crc"));
		delete(new File(seqDirectory + "/.part-00000.crc"));

		File dir = new File(seqDirectory);
		if (dir.exists()) dir.delete();
		Thread.sleep(2000);
	}

	private static void delete(File f) throws Exception {
		if (f.exists()) f.delete();
	}
}
