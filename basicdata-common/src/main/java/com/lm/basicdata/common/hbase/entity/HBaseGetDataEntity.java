package com.lm.basicdata.common.hbase.entity;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * HBase get查询返回数据封装
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/13 下午2:48
 */
@Getter
@Setter
@AllArgsConstructor
public class HBaseGetDataEntity {
    private String table;

    private String rowkey;

    private String family;

    private String qualifier;

    private String value;

    public HBaseGetDataEntity(String rowkey, String qualifier, String value) {
        this.rowkey = rowkey;
        this.qualifier = qualifier;
        this.value = value;
    }
}
