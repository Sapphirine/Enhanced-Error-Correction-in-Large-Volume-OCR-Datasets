package ee6895ta;

public class Edit extends OCRObject {

	public final char from;
	public final char to;
	
	public Edit(char from, char to) {
		super();
		this.from = from;
		this.to = to;
	}

	@Override
	public String toString() {
		return "<" + from + COMMA + to + ">";
	}
	
	public String from() {
		return String.valueOf(from);
	}
	
	public String to() {
		return String.valueOf(to);
	}
	
}
