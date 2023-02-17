package com.lm.basicdata.common.hbase.service;

import com.google.gson.Gson;
import com.lm.basicdata.common.entity.Result;
import com.lm.basicdata.common.exceptions.BusinessException;
import com.lm.basicdata.common.hbase.callable.BatchGetCallable;
import com.lm.basicdata.common.hbase.callable.GetCallable;
import com.lm.basicdata.common.hbase.entity.HBaseGetDataEntity;
import lombok.extern.slf4j.Slf4j;
import org.hbase.async.Config;
import org.hbase.async.GetResultOrException;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.logging.log4j.message.MapMessage.MapFormat.JSON;

/**
 * HBase连接服务
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/13 下午2:17
 */
@Slf4j
public class HBaseConnectService {

    /**
     * HBase连接zookeeper集合
     */
    @Value("${zklist")
    private String zkList;

    /**
     * zk导数基础路径
     */
    @Value("${basePath}")
    private String basePath;

    @Value("${hbaseGetDataTimeOut:10000}")
    private long hbaseGetDataTimeOut;

    public static HBaseClient HBASE_CLIENT;

    private final String HBASE_TABLE_COLUMN_FAMILY = "data";

    private ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    @PostConstruct
    public void initHBase() {
        if (!StringUtils.isEmpty(zkList)) {
            Config config = new Config();
            config.overrideConfig("hbase.zookeeper.quorum", zkList);
            config.overrideConfig("hbase.zookeeper.session.timeout", "5000");
            config.overrideConfig("hbase.client.retries.number", "5");
            config.overrideConfig("hbase.ipc.client.connection.idle_timeout", "60");
            config.overrideConfig("hbase.ipc.client.socket.timeout.connect.mil", "60");
            config.overrideConfig("hbase.nsre.high_watermark", "10000");
            config.overrideConfig("hbase.nsre.low_watermark", "1000");
            config.overrideConfig("hbase.rpc.timeout", "0");

            HBASE_CLIENT = new HBaseClient(config);
        }
    }

    /**
     * 根据 table、key 查询数据
     *
     * @param table 表名
     * @param key 主键
     * @return 根据主键查询到的数据
     */
    public Result<List<HBaseGetDataEntity>> getResultsFromHbase(String table, String key) {

        log.info("getResultsFromHbase, [table]: [{}], [key]: [{}]", table, key);

        Result<List<HBaseGetDataEntity>> result = new Result<>();

        List<HBaseGetDataEntity> hBaseGetDataEntities = new ArrayList<>();

        GetCallable call = new GetCallable(table, key, hbaseGetDataTimeOut);

        Future<ArrayList<KeyValue>> future = pool.submit(call);
        ArrayList<KeyValue> keyValues = null;
        try {
            keyValues = future.get();
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
            throw new BusinessException("查询失败");
        } catch (ExecutionException e) {
            log.error(e.getMessage(), e);
            throw new RuntimeException("查询失败");
        }

        if (CollectionUtils.isEmpty(keyValues)) {

            log.info("getResultsFromHbase, [table]: [{}], [key]: [{}], HBase query result is empty!", table, key);
            hBaseGetDataEntities.add(new HBaseGetDataEntity(table, key, null, null, null));
        } else {

            for (KeyValue kv : keyValues) {

                hBaseGetDataEntities.add(new HBaseGetDataEntity(
                        table,
                        key,
                        new String(kv.family()),
                        new String(kv.qualifier()),
                        new String(kv.value()))
                );
            }
        }

        result.setData(hBaseGetDataEntities);

        return result;
    }

    public Result<List<List<HBaseGetDataEntity>>> batchGetResultsFromHBase(String table, List<String> keyList,
                                                                           String family, List<String> qualifierList) {
        log.info("table: {}, keyList: {}, family: {}, qualifierList: {}", table, keyList, family, qualifierList);

        if (StringUtils.isEmpty(family)) {
            family = HBASE_TABLE_COLUMN_FAMILY;
        }

        BatchGetCallable batchGetCallable = new BatchGetCallable(table, keyList, family, qualifierList, hbaseGetDataTimeOut);
        Future<List<GetResultOrException>> listFuture = pool.submit(batchGetCallable);
        List<GetResultOrException> getResultOrExceptionList;
        try {
            getResultOrExceptionList = listFuture.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        if (CollectionUtils.isEmpty(getResultOrExceptionList)) {
            log.info("HBase query result is empty");

            return Result.success(null);
        }

        List<List<HBaseGetDataEntity>> resultData = new ArrayList<>();

        Gson gson = new Gson();
        log.info("HBase query result: {}", gson.toJson(getResultOrExceptionList));

        getResultOrExceptionList.stream().filter(
                getResult -> getResult.getException() == null
        ).forEach(kvs -> {

            ArrayList<KeyValue> cells = kvs.getCells();
            List<HBaseGetDataEntity> hBaseGetDataEntityList = new ArrayList<>(cells.size());
            for (KeyValue cell : cells) {
                HBaseGetDataEntity hBaseGetDataEntity = new HBaseGetDataEntity(
                        new String(cell.key()),
                        new String(cell.qualifier()),
                        new String(cell.value())
                );

                hBaseGetDataEntityList.add(hBaseGetDataEntity);
            }

            resultData.add(hBaseGetDataEntityList);
        });

        return Result.success(resultData);
    }


    
}

