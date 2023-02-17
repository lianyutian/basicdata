package com.lm.basicdata.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

/**
 * 网关服务启动类
 *
 * @author liming
 * @version 1.0
 * @since 2023/2/14 上午10:24
 */
@SpringBootConfiguration
@EnableAutoConfiguration
public class BasicDataGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(BasicDataGatewayApplication.class, args);
    }
}
