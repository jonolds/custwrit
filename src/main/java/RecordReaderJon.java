import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class RecordReaderJon extends RecordReader<TextJon, IntJon> {
	private TextJon key;
	private IntJon value;
	private LineRecordReader reader = new LineRecordReader();
	
	public void close() throws IOException { reader.close(); }
	public TextJon getCurrentKey() { return key; }
	public IntJon getCurrentValue() { return value; }
	
	public float getProgress() throws IOException { return reader.getProgress(); }
	
	public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException { 
		reader.initialize(is, tac); 
	}
	
	public boolean nextKeyValue() throws IOException {
		boolean gotNextKeyValue = reader.nextKeyValue();
		
		if(gotNextKeyValue){
			if(key==null){
				key = new TextJon();
			}
			if(value == null) {
				value = new IntJon();
			}
			String[] tokens = reader.getCurrentValue().toString().split(",");
			key.set(new String(tokens[0]));
			value.set(new Integer(1));
		}
		else {
			key = null;
			value = null;
		}
		return gotNextKeyValue;
	}
}