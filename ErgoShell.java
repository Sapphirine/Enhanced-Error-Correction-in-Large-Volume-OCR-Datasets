package ee6895ta;

import static ee6895ta.Ergo.SER_DIRECTORY;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import ee6895ta.TF.Rloc;

public class ErgoShell extends OCRObject {

	static String action;
	static BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
	static int Z = 3;
	static int DEFAULT_MISREADS = 3;

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: Ergo <file> ");
			System.exit(1);
		}

		String inputFile = args[0];

		Ergo ergo = Ergo.restoreFrom(SER_DIRECTORY);

		while (!action().equalsIgnoreCase("x")) {

			/*
			 * Correction
			 */
			if (action.toLowerCase().startsWith("c")) {
				String d;
				while (!(d = ask("Dominant")).equalsIgnoreCase("x")) {
					TF df = ergo.lookup(d);
					if (df.freq == 0) {
						msg("Not a token");
						continue;
					}
					msg(" -> " + df);

					msg("Variant?");
					String v = stdin.readLine();
					TF vf = ergo.lookup(v);
					if (vf.freq == 0) {
						msg("Not a token");
						continue;
					}
					msg(" -> " + vf);

					if (ergo.isInference(vf)) {
						msg(d.toString() + " -> " + v.toString() + " already inferred not a misread");
					} else {

						Rloc rloc = df.location(vf, ergo, Z, true);
						msg("z=" + Z + ":  " + d.toString() + " -> " + v.toString() + "  " + (rloc == Rloc.ABOVE ? "not a" : "plausible")
								+ " misread");
						msg(" joint log probability mass " + df.jlpm(vf, ergo));

					}
				}
			}

			/*
			 * Info
			 */
			if (action.toLowerCase().startsWith("i")) {
				String t;
				while (!(t = ask("Token")).equalsIgnoreCase("x")) {

					TF tf = ergo.lookup(t);
					msg(" -> " + tf.toString());
					msg(" -> " + (ergo.isInference(tf) ? "IS" : "NOT") + " inferred.");

				}
			}

			/*
			 * Variants
			 */
			if (action.toLowerCase().startsWith("v")) {
				String d;
				while (!(d = ask("Character (escape!)")).equalsIgnoreCase("!")) {
					for (VP vp : ergo.variantProbs(d)) {
						msg(vp.toString());
					}
				}
			}

			/*
			 * Tables
			 */
			if (action.toLowerCase().startsWith("t")) {
				msg(ergo.output(false));
			}

			/*
			 * Misreads
			 */
			if (action.toLowerCase().startsWith("m")) {
				String d;
				while (!(d = ask("Token")).equalsIgnoreCase("x")) {
					TF df = ergo.lookup(d);
					if (df.freq == 0) {
						msg("Not a token");
						continue;
					}
					msg(" -> " + df);
					(new IdentifyMisreads()).mireadsOf(df, inputFile, ergo, DEFAULT_MISREADS);

				}
			}
			
			/*
			 * Generate
			 */
			if (action.toLowerCase().startsWith("g")) {
				int cnum = Integer.parseInt(ask("number"));
				int tokenLength = Integer.parseInt(ask("length"));
				(new TopMisreads()).corrections(inputFile, ergo, cnum, tokenLength);
			}
			
			/*
			 * fix
			 */
			if (action.toLowerCase().startsWith("f")) {
				String e;
				while (!(e = ask("Token to fix")).equalsIgnoreCase("x")) {
					TF ef = ergo.lookup(e);
					if (ef.freq == 0) {
						msg("Not a token");
						continue;
					}
				
					(new Corrector()).correction(ef, inputFile, ergo);

				}
			}
		}

	}

	public static String action() throws Exception {
		msg("\nAction? (correct, info, variant, misreads, fix, generate, tables, x-escape)");
		action = stdin.readLine();
		return action;
	}

	public static String ask(String a) throws Exception {
		msg("\n" + a + "?");
		return stdin.readLine();
	}

}
