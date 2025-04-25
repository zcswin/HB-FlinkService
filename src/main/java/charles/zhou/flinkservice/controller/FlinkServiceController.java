package charles.zhou.flinkservice.controller;

import charles.zhou.flinkservice.entity.User;
import charles.zhou.flinkservice.mapper.UserMapper;
import charles.zhou.flinkservice.service.FlinkService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.tags.Tag;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/flink")
@Tag(name = "Flink Service", description = "Flink 作业相关接口")
public class FlinkServiceController {
	@Autowired
	private UserMapper userMapper;

	@PostMapping("/createUser")
	@Operation(summary = "创建用户", description = "根据传入的用户信息创建新用户")
	public User createUser(@RequestBody User user) {
		userMapper.insert(user);
		return user;
	}

    @Autowired
    private FlinkService flinkService;

    @GetMapping("/startJob")
    public String startFlinkJob() {
        try {
            // 为避免阻塞主线程，使用 CompletableFuture 异步执行
            CompletableFuture.runAsync(() -> {
                try {
                    flinkService.startFlinkJob();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            return "Flink job started successfully.";
        } catch (Exception e) {
            return "Failed to start Flink job: " + e.getMessage();
        }
    }
}
