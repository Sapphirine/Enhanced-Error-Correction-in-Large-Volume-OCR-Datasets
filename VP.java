package ee6895ta;

import java.io.Serializable;

public class VP implements Comparable<VP>, Serializable {

	private static final long serialVersionUID = -7738084869738437121L;

	public String d;
	public String v;
	public double prob;
	
	
	public VP(String d, String v, double prob) {
		super();
		this.d = d;
		this.v = v;
		this.prob = prob;
	}

	@Override
	/*
	 * Always sorted in DESC by default
	 */
	public int compareTo(VP o) {
		if (o == null) return 1;
		return o.prob > this.prob ? 1 : (this.prob != o.prob ? -1 : 0);
	}

	public String literal() {
		return d + " -> " + v + " = " + prob;
	}

	@Override
	public String toString() {
		return literal();
	}

}
