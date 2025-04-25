package charles.zhou.flinkservice;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import io.swagger.v3.oas.annotations.tags.Tag;

@SpringBootApplication
@EnableFeignClients
@EnableTransactionManagement
@Tag(name = "Flink服务提供者", description = "Flink服务提供详细描述")
@MapperScan("charles.zhou.flinkservice.mapper") // 确保包名正确
public class FlinkServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(FlinkServiceApplication.class, args);
    }
}    