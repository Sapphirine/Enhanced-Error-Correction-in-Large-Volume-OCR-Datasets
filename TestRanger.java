package ee6895ta;

import static ee6895ta.Ergo.SER_DIRECTORY;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestRanger extends OCRObject {

	public static void main(String[] args) throws Exception {

		int freq = 0;
		BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
		Ergo ergo = Ergo.restoreFrom(SER_DIRECTORY);

		msg("z?");
		double z = Double.parseDouble(stdin.readLine());

		msg("groundtruth?");
		String lit = "";

		while (!(lit = stdin.readLine()).equalsIgnoreCase("x")) {

			TF gtf = ergo.lookup(lit);
			if (gtf == null) {
				msg("freq for " + lit + "?");
				freq = Integer.parseInt(stdin.readLine());
				gtf = new TF(lit, freq);
			}

			String var = "";
			System.out.println("variant?");
			while (!(var = stdin.readLine()).equalsIgnoreCase("X")) {

				System.out.println("freq for " + var + "?");
				freq = Integer.parseInt(stdin.readLine());

				TF v = new TF(var, freq);
				WATCH_TOKEN = v.token;
				msg(gtf.toString() + " -> " + v.toString() + "  " + gtf.location(v, ergo, z, true));

				msg("\nvariant?");

			}

			msg("groundtruth?");

		}

	}

}
