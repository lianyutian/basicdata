package com.lm.basicdata.common.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 结果返回封装
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/13 下午2:58
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class Result<T> {

    private Integer code;
    private String message;
    private T data;

    public static <T> Result<T> success(T data) {
        return new Result<>(0, "success", data);
    }

}
