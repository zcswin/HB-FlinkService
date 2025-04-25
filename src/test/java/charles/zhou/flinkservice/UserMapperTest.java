package charles.zhou.flinkservice;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import charles.zhou.flinkservice.entity.User;
import charles.zhou.flinkservice.mapper.UserMapper;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = "spring.profiles.active=test")
public class UserMapperTest {    
    @Autowired
    private UserMapper userMapper;

    @Test
    public void testSelectById() {
        // 假设数据库中存在 ID 为 1 的用户
        Long userId = 1L;
        User user = userMapper.selectById(userId);
        assertNotNull(user, "根据 ID 查询用户不应为 null");
    }
}