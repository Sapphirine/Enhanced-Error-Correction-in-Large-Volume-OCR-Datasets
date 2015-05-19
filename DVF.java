package ee6895ta;

@SuppressWarnings("serial")
public class DVF extends TF {

	public DVF(String dv, int f) {
		super(dv, f);
		if (dv.length() != 2)  throw new RuntimeException("DVF must be exactly length=1: " + dv);
	}
	
	public static DVF make(char d, char v, int f) {
		char[] dvc = new char[2];
		dvc[0] = d;
		dvc[1] = v;
		return new DVF(new String(dvc), f);
	}
	
	public String d() {
		return this.token.substring(0, 1);
	}
	
	public String v() {
		return this.token.substring(1,2);
	}

	public String toString() {
		return "[ " + super.toString() + ", " + freq + "]";
	}
}
