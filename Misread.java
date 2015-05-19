package ee6895ta;

@SuppressWarnings("serial")
public class Misread extends TF {

	public double logProb;

	public Misread(String t, int f, double logProb) {
		super(t, f);
		this.logProb = logProb;
	}
	
	public Misread(TF tf, double logProb) {
		super(tf.token, tf.freq);
		this.logProb = logProb;
	}
	
	/*
	 * Always sorted in DESC by default
	 */
	public int compareTo(Misread o) {
		if (o == null) return 1;
		return o.logProb > this.logProb ? 1 : (this.logProb != o.logProb ? -1 : 0);
	}
	
}
