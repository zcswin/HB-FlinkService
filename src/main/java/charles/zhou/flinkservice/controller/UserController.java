package charles.zhou.flinkservice.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import charles.zhou.flinkservice.entity.User;
import charles.zhou.flinkservice.mapper.UserMapper;

@RestController
@RequestMapping("/flink-service")
@Tag(name = "用户管理", description = "用户相关操作接口")
public class UserController {

    @Autowired
    private UserMapper userMapper;

    @PostMapping("/createUser")
    @Operation(summary = "创建用户", description = "根据传入的用户信息创建新用户")
    public User createUser(@RequestBody User user) {
        userMapper.insert(user);
        return user;
    }
}    