package charles.zhou.flinkservice.controller;

import charles.zhou.flinkservice.entity.User;
import charles.zhou.flinkservice.mapper.UserMapper;
import charles.zhou.flinkservice.service.FlinkService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/flink")
@Tag(name = "Flink Service", description = "Flink 作业相关接口")
public class FlinkServiceController {

    @Autowired
    private UserMapper userMapper;

    @PostMapping("/createUser")
    @Operation(summary = "创建用户", description = "根据传入的用户信息创建新用户")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "用户创建成功",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = User.class)) })
    })
    public User createUser(@RequestBody @Parameter(description = "用户信息", required = true) User user) {
        userMapper.insert(user);
        return user;
    }

    @Autowired
    private FlinkService flinkService;

    @GetMapping("/startJob")
    @Operation(summary = "启动 Flink 作业", description = "异步启动 Flink 作业")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Flink 作业启动成功",
                    content = { @Content(mediaType = "text/plain",
                            schema = @Schema(implementation = String.class)) }),
            @ApiResponse(responseCode = "500", description = "Flink 作业启动失败",
                    content = { @Content(mediaType = "text/plain",
                            schema = @Schema(implementation = String.class)) })
    })
    public String startFlinkJob() {
        try {
            flinkService.startFlinkJob();
            return "Flink job started successfully.";
        } catch (Exception e) {
            return "Failed to start Flink job: " + e.getMessage();
        }
    }

    private Object waitForJobAndGetResult() {
        while (flinkService.isJobRunning()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        String errorMessage = flinkService.getErrorMessage();
        if (errorMessage != null) {
            return errorMessage;
        }
        return null;
    }

    @GetMapping("/groupedResults")
    @Operation(summary = "获取分组结果", description = "获取 Flink 作业的分组结果")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "成功获取分组结果",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class)) }),
            @ApiResponse(responseCode = "500", description = "作业执行失败",
                    content = { @Content(mediaType = "text/plain",
                            schema = @Schema(implementation = String.class)) })
    })
    public Object getGroupedResults() {
        Object error = waitForJobAndGetResult();
        if (error != null) {
            return error;
        }
        return flinkService.getGroupedResults();
    }

    @GetMapping("/statefulResults")
    @Operation(summary = "获取状态管理结果", description = "获取 Flink 作业的状态管理结果")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "成功获取状态管理结果",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class)) }),
            @ApiResponse(responseCode = "500", description = "作业执行失败",
                    content = { @Content(mediaType = "text/plain",
                            schema = @Schema(implementation = String.class)) })
    })
    public Object getStatefulResults() {
        Object error = waitForJobAndGetResult();
        if (error != null) {
            return error;
        }
        return flinkService.getStatefulResults();
    }

    @GetMapping("/filteredResults")
    @Operation(summary = "获取过滤结果", description = "获取 Flink 作业的过滤结果")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "成功获取过滤结果",
                    content = { @Content(mediaType = "application/json",
                            schema = @Schema(implementation = List.class)) }),
            @ApiResponse(responseCode = "500", description = "作业执行失败",
                    content = { @Content(mediaType = "text/plain",
                            schema = @Schema(implementation = String.class)) })
    })
    public Object getFilteredResults() {
        Object error = waitForJobAndGetResult();
        if (error != null) {
            return error;
        }
        return flinkService.getFilteredResults();
    }

    @GetMapping("/jobStatus")
    @Operation(summary = "获取作业状态和进度", description = "获取 Flink 作业的当前状态和进度")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "成功获取作业状态和进度",
                    content = { @Content(mediaType = "text/plain",
                            schema = @Schema(implementation = String.class)) })
    })
    public String getJobStatus() {
        if (flinkService.isJobRunning()) {
            return "Job is running. Progress: " + flinkService.getProgress() + "%";
        }
        String errorMessage = flinkService.getErrorMessage();
        if (errorMessage != null) {
            return errorMessage;
        }
        return "Job is not running.";
    }
}    