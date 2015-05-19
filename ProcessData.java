package ee6895ta;

import static ee6895ta.Ergo.SER_DIRECTORY;

import java.util.List;

import scala.Tuple2;

public class ProcessData extends OCRUtility {

	public static final String[] PHASES = new String[] { "STATUS", "INIT", "INFER" };

	public static void main(String[] args) throws Exception {

		Ergo ergo;

		if (args.length < 3) {
			System.err.println("Usage: ProcessData <file> <max-token-length> <phase>");
			System.exit(1);
		}

		String inputFile = args[0];
		Integer tokenLen = Integer.parseInt(args[1]);
		String phase = args[2];

		/*
		 * STATUS: Just report
		 */
		if (phase.equalsIgnoreCase(PHASES[0])) {
			ergo = Ergo.restoreFrom(SER_DIRECTORY);
			msg(ergo.output(true));
			System.exit(0);
		}

		/*
		 * INIT: Raw frequencies
		 */
		if (phase.equalsIgnoreCase(PHASES[1])) {
			ergo = new Ergo();
			InitRawFrequencies initer = new InitRawFrequencies();
			initer.initializeChars(inputFile, ergo);

			for (int currentLength = MIN_TOKEN_LENGTH; currentLength <= tokenLen; currentLength++) {
				
				initer.collectRawFrequencies(inputFile, ergo, currentLength);
				
				for (Tuple2<String, Integer> t : initer.list_charfreqs) {
					ergo.rawCharOccurrence(t._1, t._2);
				}
				int r = ergo.cacheTokens(initer.list_tfs);
				verbose("Ergo caches " + r + " token frequencies.");
				r = ergo.cacheTop(initer.tops);
				verbose("Ergo caches " + r + " top tokens.");
			}

			for (int currentLength = MIN_TOKEN_LENGTH; currentLength <= tokenLen; currentLength++) {
				InitRawVariants irv = new InitRawVariants(currentLength);
				List<DVF> dvfs = irv.rawVariants(inputFile, ergo, currentLength);
				ergo.declareVariants(dvfs);
			}

			ergo.saveTo(SER_DIRECTORY);
			msg(ergo.output(true));
		}

		/*
		 * INFER:
		 */
		if (phase.equalsIgnoreCase(PHASES[2])) {
			ergo = Ergo.restoreFrom(SER_DIRECTORY);
			for (int currentLength = MIN_TOKEN_LENGTH; currentLength <= tokenLen; currentLength++) {
				InferTruth infer = new InferTruth(currentLength);
				ergo.declareInferences(infer.inferences(inputFile, ergo, currentLength));
			}
			ergo.saveTo(SER_DIRECTORY);
		}

	}

}
