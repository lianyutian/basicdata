package com.lm.basicdata.common.hbase.callable;

import com.lm.basicdata.common.hbase.service.HBaseConnectService;
import lombok.AllArgsConstructor;
import org.hbase.async.Bytes;
import org.hbase.async.GetRequest;
import org.hbase.async.GetResultOrException;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * BatchGetCallable
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/14 下午3:33
 */
@AllArgsConstructor
public class BatchGetCallable implements Callable<List<GetResultOrException>> {

    private String table;
    private List<String> rowKeyList;
    private String family;
    private List<String> qualifierList;
    private long timeout;

    @Override
    public List<GetResultOrException> call() throws Exception {
        List<GetRequest> getRequestList = new ArrayList<>(rowKeyList.size());

        byte[][] qualifiers = new byte[0][];
        boolean qualifierListEmptyFlag = true;

        if (!CollectionUtils.isEmpty(qualifierList)) {
            qualifierListEmptyFlag = false;
            qualifiers = new byte[qualifierList.size()][];
            for (int i = 0; i < qualifierList.size(); i++) {
                qualifiers[i] = Bytes.UTF8(qualifierList.get(i));
            }
        }

        for (String rowKey : rowKeyList) {
            if (StringUtils.isEmpty(rowKey)) {
                continue;
            }

            GetRequest getRequest = new GetRequest(table, rowKey, family);
            if (!qualifierListEmptyFlag) {
                getRequest.qualifiers(qualifiers);
            }

            getRequestList.add(getRequest);
        }

        return HBaseConnectService.HBASE_CLIENT.get(getRequestList).join();
    }
}
