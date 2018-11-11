package com.creditease.dbus.rabbitmq.forward.container;

import com.creditease.dbus.rabbitmq.forward.domain.JdbcProperties;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;

import static com.creditease.dbus.commons.Constants.CONFIG_DB_KEY;

public class DataSourceContainer {
    private static final Logger logger = LoggerFactory.getLogger(DataSourceContainer.class);

    private static DataSourceContainer container;

    private ConcurrentHashMap<String, BasicDataSource> cmap = new ConcurrentHashMap<String, BasicDataSource>();

    private DataSourceContainer() {
    }

    public static DataSourceContainer getInstances() {
        if (container == null) {
            synchronized (DataSourceContainer.class) {
                if (container == null){
                    container = new DataSourceContainer();
                }
            }
        }
        return container;
    }

    public Connection getConn(String key) {
        Connection conn = null;
        try {
            if (cmap.containsKey(key))
                conn = cmap.get(key).getConnection();
        } catch (SQLException e) {
        }
        return conn;
    }

    public boolean initDataSource(JdbcProperties jdbcProperties) {
        boolean isOk = true;
        try {
            BasicDataSource bds = new BasicDataSource();
            bds.setDriverClassName(jdbcProperties.getDriverClass());
            bds.setUrl(jdbcProperties.getUrl());
            bds.setUsername(jdbcProperties.getUserName());
            bds.setPassword(jdbcProperties.getPassword());
            bds.setInitialSize(jdbcProperties.getInitialSize());
            bds.setMaxActive(jdbcProperties.getMaxActive());
            bds.setMaxIdle(jdbcProperties.getMaxIdle());
            bds.setMinIdle(jdbcProperties.getMinIdle());
            cmap.put(CONFIG_DB_KEY, bds);
        } catch (Exception e) {
            isOk = false;
        }
        return isOk;
    }

}
