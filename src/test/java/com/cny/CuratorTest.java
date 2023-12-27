package com.cny;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@Slf4j
@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CuratorTest {

    private CuratorFramework client;

    @BeforeAll
    public void init() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient("192.168.247.5:2181", retryPolicy);
        client.start();
    }

    @Test
    public void connectTest() {
        log.info("connect success ： {}", client.toString());
    }

    @Test
    public void createTest() throws Exception {
        client.create()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/curatortest", "hello,curator".getBytes());
        log.info("create success");
    }

    @Test
    public void existTest() throws Exception {
        Stat stat1 = client.checkExists().forPath("/curatortest");
        System.out.println(stat1 == null);

        Stat stat2 = client.checkExists().forPath("/curatortest333");
        System.out.println(stat2 == null);
    }

    @Test
    public void updateTest() throws Exception {
        client.setData()
                .forPath("/curatortest", "9999".getBytes());
    }

    @Test
    public void deleteTest() throws Exception {
        client.delete()
                .forPath("/curatortest");
    }

    // 一次性监听
    @Test
    public void watch1Test() throws Exception {
        client.getData()
                .usingWatcher(new CuratorWatcher() {
                    @Override
                    public void process(WatchedEvent watchedEvent) throws Exception {
                        log.info("{}发生了变化{}", watchedEvent.getState(), watchedEvent.getType());
                    }
                }).forPath("/curatortest");
        System.in.read();
    }


    // 对一个节点进行监听 重复监听
    @Test
    public void watch2Test() throws Exception {
        NodeCache nodeCache = new NodeCache(client, "/curatortest");
        nodeCache.start();
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("{}发生了变化，数据变化为：{}", nodeCache.getCurrentData().getPath(), new String(nodeCache.getCurrentData().getData()));
            }
        });
        System.in.read();
    }


    // 对一个子节点进行监听 重复监听
    @Test
    public void watch3Test() throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/curatortest", true);
        pathChildrenCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED:
                        log.info("增加了子节点：{}, 数据为：{}", event.getData().getPath(), new String(event.getData().getData()));
                        break;
                    case CHILD_UPDATED:
                        log.info("子节点：{} 更新了，数据为：{}", event.getData().getPath(), new String(event.getData().getData()));
                        break;
                    case CHILD_REMOVED:
                        log.info("子节点：{} 被删除了", event.getData().getPath());
                        break;
                    default:
                        break;
                }
            }
        });
        System.in.read();
    }


    // 可监听根节点和子节点
    @Test
    public void watch4Test() throws Exception {
        TreeCache treeCache = TreeCache.newBuilder(client, "/curatortest")
                //设置监听的深度
                //0:监听的当前节点
                //1:监听到直属子节点
                //.setMaxDepth(0)
                .build();

        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                if (event.getData() != null) {
                    log.info("path:{}, type:{}, data:{}", event.getData().getPath(), event.getType(), new String(event.getData().getData()));
                } else {
                    log.info("type: {}", event.getType());
                }
            }
        });

        treeCache.start();

        System.in.read();
    }

}
