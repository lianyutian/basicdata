package com.lm.basicdata.common.exceptions;

/**
 * TODO
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/14 下午2:02
 */
public class BusinessException extends RuntimeException {
    public BusinessException() {

    }

    public BusinessException(String message) {
        super(message);
    }
}
