import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileInputFormatJon extends FileInputFormat<IntJon, TextJon> {
	public RecordReader<IntJon, TextJon> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) {
		try { System.out.println(arg1.getInputFormatClass().getSimpleName());
		} catch (ClassNotFoundException e) { e.printStackTrace(); }
		
		return new RecordReaderJon();
	}
}