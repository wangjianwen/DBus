package com.creditease.dbus.rabbitmq.forward.dao;

import com.creditease.dbus.domain.model.DataSchema;
import com.creditease.dbus.rabbitmq.forward.domain.ExchangeProperties;

import java.util.List;

public interface LoadRabbitmqDao {
    List<ExchangeProperties> getExchangePropertiesList(String dnsName);
    DataSchema getSchema(String dnsName);
}
