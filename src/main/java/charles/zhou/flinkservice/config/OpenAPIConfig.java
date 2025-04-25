package charles.zhou.flinkservice.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenAPIConfig {    
    @Bean
    OpenAPI customOpenAPI() {
        return new OpenAPI()
               .info(new Info()
                       .title("Flink Service API")
                       .description("Flink 作业相关接口文档")
                       .version("1.0.0"));
    }
}
