package charles.zhou.flinkservice.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import charles.zhou.flinkservice.entity.User;
import charles.zhou.flinkservice.mapper.UserMapper;
import org.springframework.stereotype.Service;

@Service
public class UserService extends ServiceImpl<UserMapper, User> {
    // 可以自定义一些方法来处理业务逻辑
}