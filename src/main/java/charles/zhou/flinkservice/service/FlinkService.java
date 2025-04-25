package charles.zhou.flinkservice.service;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import charles.zhou.flinkservice.entity.User;
import charles.zhou.flinkservice.mapper.UserMapper;
import jakarta.annotation.PostConstruct;

import java.util.Arrays;

@Service
public class FlinkService {

    @Autowired
    private UserMapper userMapper;

    @PostConstruct
    public void startFlinkJob() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.fromCollection(Arrays.asList("user1,user1@example.com", "user2,user2@example.com", "user3,user3@example.com"));

        dataStream.map(data -> {
            String[] parts = data.split(",");
            User user = new User();
            user.setName(parts[0]);
            user.setEmail(parts[1]);
            userMapper.insert(user);
            return user;
        }).print();

        env.execute("Flink MicroService App");
    }
}    