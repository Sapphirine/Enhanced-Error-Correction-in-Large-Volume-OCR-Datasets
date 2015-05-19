package ee6895ta;

public class Range {

	private int rmin;
	private int rmax;

	public Range(int min, int max) {
		super();
		this.rmin = min;
		this.rmax = max;
	}

	public Range(double min, double max) {
		setMin(min);
		setMax(max);
	}

	public int getMin() {
		return rmin;
	}

	public int getMax() {
		return rmax;
	}

	public void setMin(int min) {
		this.rmin = (min < 1? 0 : min);
	}

	public void setMax(int max) {
		this.rmax = max;
	}

	public void setMin(double min) {
		setMin((int) Math.round(min));
	}

	public void setMax(double max) {
		setMax((int) Math.round(max));
	}

	@Override
	public String toString() {
		return "[ " +rmin + " , " + rmax + " ]";
	}

}
