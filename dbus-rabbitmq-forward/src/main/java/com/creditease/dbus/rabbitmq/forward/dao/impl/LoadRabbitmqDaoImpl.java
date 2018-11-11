package com.creditease.dbus.rabbitmq.forward.dao.impl;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.rabbitmq.forward.container.DataSourceContainer;
import com.creditease.dbus.rabbitmq.forward.dao.LoadRabbitmqDao;
import com.creditease.dbus.rabbitmq.forward.domain.ExchangeProperties;
import org.apache.storm.shade.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class LoadRabbitmqDaoImpl implements LoadRabbitmqDao {

    @Override
    public List<ExchangeProperties> getExchangePropertiesList(String dnsName) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExchangeProperties> list = Lists.newArrayList();
        try {
            conn = DataSourceContainer.getInstances().getConn(Constants.CONFIG_DB_KEY);
            ps = conn.prepareStatement("select id, ds_id, schema_id, schema_name, exchange_name, type, durable, auto_delete " +
                    " from t_dbus_rabbitmq_exchange");

            rs = ps.executeQuery();
            while (rs.next()) {
                ExchangeProperties exchangeProperties = new ExchangeProperties();
                exchangeProperties.setName(rs.getString("exchange_name"));
                exchangeProperties.setType(rs.getString("type"));
                exchangeProperties.setDurable(rs.getBoolean("durable"));
                exchangeProperties.setAutoDelete(rs.getBoolean("auto_delete"));
                list.add(exchangeProperties);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            close(rs);
            close(ps);
            close(conn);
        }
        return list;
    }

    @Override
    public DataSchema getSchema(String dnsName){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<DataSchema> list = Lists.newArrayList();
        try {
            conn = DataSourceContainer.getInstances().getConn(Constants.CONFIG_DB_KEY);
            ps = conn.prepareStatement("select id, ds_id, schema_name, status, src_topic, target_topic, create_time, description " +
                    " from t_data_schema");

            rs = ps.executeQuery();
            while (rs.next()) {
                DataSchema dataSchema = new DataSchema();
                dataSchema.setId(rs.getInt("id"));
                dataSchema.setDsId(rs.getInt("ds_id"));
                dataSchema.setSchemaName(rs.getString("schema_name"));
                dataSchema.setStatus(rs.getString("auto_delete"));
                list.add(dataSchema);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            close(rs);
            close(ps);
            close(conn);
        }
        return list.get(0);
    }

    static void close(Object obj) {
        if (obj == null)
            return;
        try {
            if (obj instanceof PreparedStatement)
                ((PreparedStatement) obj).close();
            if (obj instanceof ResultSet)
                ((ResultSet) obj).close();
            if (obj instanceof Connection)
                ((Connection) obj).close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
