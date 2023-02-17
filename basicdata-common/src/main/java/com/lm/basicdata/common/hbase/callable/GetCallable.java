package com.lm.basicdata.common.hbase.callable;

import com.lm.basicdata.common.hbase.service.HBaseConnectService;
import lombok.AllArgsConstructor;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;

import java.util.ArrayList;
import java.util.concurrent.Callable;

/**
 * GetCallable
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/13 下午3:17
 */
@AllArgsConstructor
public class GetCallable implements Callable<ArrayList<KeyValue>> {

    private String table;
    private String rowkey;
    private long timeout;

    @Override
    public ArrayList<KeyValue> call() throws Exception {
        GetRequest getRequest = new GetRequest(table, rowkey);
        return HBaseConnectService.HBASE_CLIENT.get(getRequest).join(timeout);
    }
}
