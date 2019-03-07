import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class RecordReaderJon extends RecordReader<IntJon, TextJon> {
	private TextJon value;
	private IntJon key;
	private LineRecordReader reader = new LineRecordReader();
	
	public void close() throws IOException { reader.close(); }
	public IntJon getCurrentKey() { return key; }
	public TextJon getCurrentValue() { return value; }
	
	public float getProgress() throws IOException { return reader.getProgress(); }
	
	public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException { 
		reader.initialize(is, tac); 
	}
	
	public boolean nextKeyValue() throws IOException {
		boolean gotNextKeyValue = reader.nextKeyValue();
		
		if(gotNextKeyValue) {
			if(key==null)
				key = new IntJon();
			if(value == null)
				value = new TextJon();
			String[] tokens = reader.getCurrentValue().toString().split(",");
			value.set(new String(tokens[0]));
			key.set(new Integer(99));
		}
		else {
			key = null;
			value = null;
		}
		return gotNextKeyValue;
	}
}