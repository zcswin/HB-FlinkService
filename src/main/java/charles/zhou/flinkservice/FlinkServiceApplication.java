package charles.zhou.flinkservice;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.tuple.Tuple2;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import charles.zhou.flinkservice.service.FlinkService;
import io.swagger.v3.oas.annotations.tags.Tag;

@SpringBootApplication
@EnableFeignClients
@EnableTransactionManagement
@Tag(name = "Flink服务提供者", description = "Flink服务提供详细描述")
@MapperScan("charles.zhou.flinkservice.mapper") // 确保包名正确
public class FlinkServiceApplication implements CommandLineRunner{
    public static void main(String[] args) {
        SpringApplication.run(FlinkServiceApplication.class, args);
    }
    @Autowired
    private FlinkService flinkService;

    @Override
    public void run(String... args) throws Exception {
        flinkService.startFlinkJob();
        List<Tuple2<String, Integer>> groupedResults = flinkService.getGroupedResultsSync(60, TimeUnit.SECONDS);
        List<Tuple2<String, Integer>> statefulResults = flinkService.getStatefulResultsSync(60, TimeUnit.SECONDS);
        List<Tuple2<String, String>> filteredResults = flinkService.getFilteredResultsSync(60, TimeUnit.SECONDS);

        System.out.println("Grouped Results: " + groupedResults);
        System.out.println("Stateful Results: " + statefulResults);
        System.out.println("Filtered Results: " + filteredResults);
    }
}    