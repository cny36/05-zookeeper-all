package com.cny;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Slf4j
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZKTest {

    private ZooKeeper zookeeper;

    @SneakyThrows
    @BeforeAll
    private void init() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        zookeeper = new ZooKeeper("192.168.247.5:2181", 3000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // WatchedEvent state:SyncConnected type:None path:null
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected
                        && watchedEvent.getType() == Event.EventType.None) {
                    log.info("连接服务器成功");
                    countDownLatch.countDown();
                } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                    log.info("数据发生了变更");
                } else {
                    log.info("连接服务器失败");
                }
            }
        });
        log.info("正在连接服务器中。。。");
        countDownLatch.await();
    }

    @Test
    public void connectTest() {
        log.info("测试连接服务器成功");
    }


    @Test
    public void createTest() throws IOException, InterruptedException, KeeperException {
        byte[] bytes = new ObjectMapper().writeValueAsBytes(new CommonConfigDTO("url", "https://www.baidu.com"));
        String result = zookeeper.create("/config", bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("创建节点成功，节点路径为：{}", result);
    }

    @Test
    public void getDataTest() throws IOException, InterruptedException, KeeperException {
        byte[] data = zookeeper.getData("/config", null, null);
        CommonConfigDTO commonConfigDTO = new ObjectMapper().readValue(data, CommonConfigDTO.class);
        log.info("获取到的数据为：{}", commonConfigDTO);
    }

    @Test
    public void updateDataTest() throws IOException, InterruptedException, KeeperException {
        byte[] bytes = new ObjectMapper().writeValueAsBytes(new CommonConfigDTO("url2", "https://www.taobao.com"));
        // version -1 表示不采用乐观锁机制修改
        Stat result = zookeeper.setData("/config", bytes, -1);
        log.info("更新数据成功，版本号为：{}", result.getVersion());
    }

    @Test
    public void updateDataWithVersionTest() throws IOException, InterruptedException, KeeperException {
        // 1.获取数据
        Stat stat = new Stat();
        zookeeper.getData("/config", null, stat);

        // 2.修改数据
        byte[] data = new ObjectMapper().writeValueAsBytes(new CommonConfigDTO("url", "https://www.hello.com"));
        zookeeper.setData("/config", data, stat.getVersion());

        log.info("更新数据成功，{}", stat.getVersion());
    }

    @Test
    public void deleteTest() throws IOException, InterruptedException, KeeperException {
        zookeeper.delete("/config", -1);
        log.info("删除成功");
    }

    @Test
    public void watchTest() throws IOException, InterruptedException, KeeperException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Watcher watcher = new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                    log.info("节点数据发生了变更");
                }
            }
        };

        byte[] data = zookeeper.getData("/config", watcher, null);
        CommonConfigDTO commonConfigDTO = new ObjectMapper().readValue(data, CommonConfigDTO.class);
        log.info("获取到的数据为：{}", commonConfigDTO);
        countDownLatch.await();
    }

}
