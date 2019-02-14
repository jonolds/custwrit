import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FileInputFormatJon extends FileInputFormat<TextJon, IntJon> {

	public RecordReader<TextJon, IntJon> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) {
		return new RecordReaderJon();
	}
}