-- 创建 users 表
CREATE TABLE IF NOT EXISTS users (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255)
);

-- 插入示例数据
INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john.doe@example.com');