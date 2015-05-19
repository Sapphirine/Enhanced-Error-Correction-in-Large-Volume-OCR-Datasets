package ee6895ta;

import java.util.ArrayList;

public abstract class OCRObject {

	/*
	 * ==============================================
	 * System Parameters
	 * ==============================================
	 */
	public static final boolean VERBOSE = true;
	public static final int ERROR_RADIUS = 3;
	public static final int SEARCH_RADIUS = 2;
	public static final int MIN_TOKEN_LENGTH = 3;
	public static final int MAX_TOKEN_LENGTH = 9;
	public static final String CO_DOMINANCE = "CODOMINANCE,";
	public static final int DOMINANCE_MULTIPLE = 10;
	public static final int MIN_POP_SAMPLE_RATIO = 10;
	public static final double MAX_GLOBAL_MISREAD_RATE = 0.15;
	public static final double CONFIDENCE_LEVEL = 0.95;
	public static final double DEFACTO_ONE = 0.9999999;
	public static final double EPSILON = 0.00000001;
	public static final int MIN_SUPPORT_FOR_INFERENCE = 5000;
	public static final int CACHE_TOP_LEN = 50;
	

	/*
	 * ==============================================
	 * Literals
	 * ==============================================
	 */
	public static final char STAR = '*';
	public static final String COMMA = ",";
	public static final String STAR_STRING = "*";
	public static final String EQUALS = "=";
	public static final String BLANK = "";

	/*
	 * ------------------------------------------------------------------------------------------
	 * Debug
	 * ------------------------------------------------------------------------------------------
	 */
	
	public static String WATCH_TOKEN = "";

	/*
	 * -------------------------------
	 * Collect the edits required to go from (proposed) truth t to (proposed) variant v
	 * -------------------------------
	 */
	public static ArrayList<Edit> edits(String t, String v) {
		assert (t.length() == v.length());
		ArrayList<Edit> edits = new ArrayList<Edit>();
		char[] tcs = t.toCharArray();
		char[] vcs = v.toCharArray();
		for (int i = 0; i < tcs.length; i++) {
			if (tcs[i]!=vcs[i]) {
				edits.add(new Edit(tcs[i], vcs[i]));
			}
		}
		return edits;
	}

	/*
	 * -------------------------------
	 * Count stars
	 * -------------------------------
	 */
	public static int starcount(String s) {
		int sc = 0;
		for (char c : s.toCharArray()) {
			sc += (c == STAR ? 1 : 0);
		}
		return sc;
	}

	/*
	 * -------------------------------
	 * msg
	 * -------------------------------
	 */
	public static void msg(String s) {
		System.out.println(s);
	}
	
	/*
	 * -------------------------------
	 * msg with break line before
	 * -------------------------------
	 */
	public static void msgb(String s) {
		msg("\n" + s);
	}

	/*
	 * --------------------------------
	 * msg same line
	 * --------------------------------
	 */
	public static void msgl(String s) {
		System.out.print(s);
	}
	
	public static void verbose(String s) {
		if (VERBOSE) System.out.println(s);
	}
	
	public static void verbose(String watchToken, String s) {
		if (VERBOSE && watchToken.equals(WATCH_TOKEN)) System.out.println(s);
	}
	
	public static void verbosel(String s) {
		if (VERBOSE) System.out.print(s);
	}
	public static void verbose(boolean emit, String s) {
		if (emit) msg(s);
	}
}
