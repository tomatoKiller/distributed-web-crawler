package SearchEngine.DataStructure;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by wu on 14-6-29.
 */

public class url_data implements WritableComparable<url_data> {

    private byte status;
//    private String url;   //由于在key值中已经有了url了，因此url_data中不在需要一个重复的url，可以节省空间
    private Long lastFetchTime;

    /** Page was not fetched yet. */
    public static final byte STATUS_DB_UNFETCHED      = 0x01;
    /** Page was successfully fetched. */
    public static final byte STATUS_DB_FETCHED        = 0x02;
    /** Page no longer exists. */
    public static final byte STATUS_DB_GONE           = 0x03;
    /** Page temporarily redirects to other page. */
    public static final byte STATUS_DB_REDIR_TEMP     = 0x04;
    /** Page permanently redirects to other page. */
    public static final byte STATUS_DB_REDIR_PERM     = 0x05;
    /** Page was successfully fetched and found not modified. */
    public static final byte STATUS_DB_READYTOFETCH   = 0x06;

    /** Maximum value of DB-related status. */
    public static final byte STATUS_DB_MAX            = 0x1f;

    /** Fetching was successful. */
    public static final byte STATUS_FETCH_SUCCESS     = 0x21;
    /** Fetching unsuccessful, needs to be retried (transient errors). */
    public static final byte STATUS_FETCH_RETRY       = 0x22;
    /** Fetching temporarily redirected to other page. */
    public static final byte STATUS_FETCH_REDIR_TEMP  = 0x23;
    /** Fetching permanently redirected to other page. */
    public static final byte STATUS_FETCH_REDIR_PERM  = 0x24;
    /** Fetching unsuccessful - page is gone. */
    public static final byte STATUS_FETCH_GONE        = 0x25;
    /** Fetching successful - page is not modified. */
    public static final byte STATUS_FETCH_NOTMODIFIED = 0x26;

    /** Maximum value of fetch-related status. */
    public static final byte STATUS_FETCH_MAX         = 0x3f;

    /** Page signature. */
    public static final byte STATUS_SIGNATURE         = 0x41;
    /** Page was newly injected. */
    public static final byte STATUS_INJECTED          = 0x42;
    /** Page discovered through a link. */
    public static final byte STATUS_LINKED            = 0x43;

    public url_data(){}

    public url_data(url_data ul) {
        this.status = ul.status;
        this.lastFetchTime = ul.lastFetchTime;
    }

    public url_data(byte status) {
        this.status = status;

    }

    public void set(url_data ul) {
        this.status = ul.status;
        this.lastFetchTime = ul.lastFetchTime;
    }

    public void setStatus(byte status) {

        this.status = status;
    }

    public void setLastFetchTime(Long lastFetchTime) {
        this.lastFetchTime = lastFetchTime;
    }

    public byte getStatus() {

        return status;
    }

    public Long getLastFetchTime() {
        return lastFetchTime;
    }


    @Override
    public int compareTo(url_data other) {

        //目前，没发现该比较的用处
        if (other.status != this.status) {
            return other.status - this.status;
        }
        if(this.lastFetchTime!=other.lastFetchTime)
            return (other.lastFetchTime-this.lastFetchTime)>0?1:-1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeByte(status);
        dataOutput.writeLong(lastFetchTime);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.status = dataInput.readByte();
        this.lastFetchTime = dataInput.readLong();
    }
}
