package SearchEngine.DataStructure;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wu on 2014/7/2.
 */
public class DocumentWritable implements Writable {

    private long lastFetchTime;  //最后抓取时间
    private String content;

    public DocumentWritable(String content, long lastFetchTime) {
        this.content = content;
        this.lastFetchTime = lastFetchTime;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(lastFetchTime);
        dataOutput.writeBytes(content);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        lastFetchTime = dataInput.readLong();
        content = dataInput.toString();
    }
}
