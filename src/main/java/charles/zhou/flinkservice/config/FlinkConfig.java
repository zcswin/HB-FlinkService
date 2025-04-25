package charles.zhou.flinkservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import charles.zhou.flinkservice.service.FlinkService;

@Configuration
public class FlinkConfig {
    @Bean
    // 确保没有设置成 prototype 作用域
    FlinkService flinkService() {
        return new FlinkService();
    }
}