import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class IntJon implements Writable, WritableComparable<IntJon>{
	private IntWritable value;

	public IntJon(IntWritable val) { this.value = val; }
	public IntJon(int val) { this(new IntWritable(val)); }
	public IntJon() { this.value = new IntWritable(); }

	public static IntJon read(DataInput in) throws IOException {
		IntJon newIntJon = new IntJon();
		newIntJon.readFields(in);
		return newIntJon;
	}

	public void write(DataOutput out) throws IOException { value.write(out); }
	public void readFields(DataInput in) throws IOException { value.readFields(in); }

	public String toString() { return value.toString(); }

	public int compareTo(IntJon o) { return value.compareTo(o.getValue()); }
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IntJon newIntJon = (IntJon) o;
		return value.equals(newIntJon.getValue()); 
	}

	public int hashCode() { return 163 * (value != null ? value.hashCode() : 0); }
	public void set(int val){ this.value.set(val); }
	public IntWritable getValue() { return value; }
	public int get() { return value.get(); }
}