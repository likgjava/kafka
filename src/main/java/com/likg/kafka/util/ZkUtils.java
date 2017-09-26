package com.likg.kafka.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * zk 工具类
 */
public class ZkUtils {

    protected static final Logger log = LoggerFactory.getLogger(ZkUtils.class);
    private static CuratorFramework client;

    public static void init(final String zkString) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 1);
        client = CuratorFrameworkFactory.newClient(zkString, retryPolicy);
        CuratorListener listener = new CuratorListener() {

            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
                if (event == null) {
                    return;
                }
            }
        };
        client.getCuratorListenable().addListener(listener);
        client.start();
    }


    public static boolean exists(String path) {
        try {
            Stat stat = client.checkExists().forPath(path);
            return stat == null ? false : true;
        } catch (Exception e) {
            log.error("Failed to check whether the node exists." + path, e);
        }
        return false;
    }

    public static CuratorFramework getClient() {
        return client;
    }

    public static List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            log.error("Failed to get children of node " + path, e);
        }
        return null;
    }

    public static boolean createE(String path, String data) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(path, data.getBytes());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean createP(String path, String data) {
        try {
            client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path, data.getBytes());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean setP(String path, String data) {
        try {
            client.setData().forPath(path, data.getBytes());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static List<String> getP(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            return null;
        }
    }

    public static List<String> getChildrenAndListen(String path, Watcher watcher) {
        try {
            return client.getChildren().usingWatcher(watcher).forPath(path);
        } catch (Exception e) {
            log.error("Failed to get children of node " + path, e);
        }
        return null;
    }

    public static void delete(String path) throws Exception {
        try {
            List<String> childList = client.getChildren().forPath(path);
            if (childList == null || childList.isEmpty()) {
                client.delete().forPath(path);
            } else {
                for (String childName : childList) {
                    String childPath = path + "/" + childName;
                    List<String> grandChildList = client.getChildren().forPath(childPath);
                    if (grandChildList == null || grandChildList.isEmpty()) {
                        client.delete().forPath(childPath);
                    } else {
                        delete(childPath);
                    }
                }
                client.delete().forPath(path);
            }
        } catch (Exception e) {
            log.error("Failed to delete node recursively", e);
            throw e;
        }
    }

    public static boolean isParent(String path) {
        return getChildren(path) != null && !getChildren(path).isEmpty();
    }

    public static String getContent(String path) {
        try {
            return new String(client.getData().forPath(path));
        } catch (Exception e) {
            log.error("Failed to get date of node " + path, e);
        }
        return null;
    }

    public static void main(String[] args) {
        ZkUtils.init("192.168.1.238:2181/config/mobile/mq");
        List<String> children = getChildren("/consumers/group-stat/offsets/stat");
        System.out.println(children);


        String content = getContent("/consumers/group-stat/offsets/stat/0");
        System.out.println(content);


    }

}

