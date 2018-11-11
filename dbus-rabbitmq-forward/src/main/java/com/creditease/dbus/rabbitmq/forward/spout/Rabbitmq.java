package com.creditease.dbus.rabbitmq.forward.spout;

import com.creditease.dbus.commons.Constants;
import com.creditease.dbus.rabbitmq.forward.domain.ExchangeProperties;
import com.creditease.dbus.rabbitmq.forward.domain.RabbitmqProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;

class Rabbitmq {
    private RabbitmqProperties rabbitmqProperties;

    static Rabbitmq createInstance(RabbitmqProperties rabbitmqProperties){
        return new Rabbitmq(rabbitmqProperties);
    }

    private Rabbitmq(RabbitmqProperties rabbitmqProperties){
        this.rabbitmqProperties = rabbitmqProperties;
    }

    public Channel createChannel() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitmqProperties.getHost());
        factory.setPort(rabbitmqProperties.getPort());
        factory.setUsername(rabbitmqProperties.getUserName());
        factory.setPassword(rabbitmqProperties.getPassword());
        try {
            Connection connection = factory.newConnection();
            return connection.createChannel();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void exchangeDeclareAndBindQueue(Channel channel, ExchangeProperties exchangeProperties) {
        try {
            channel.exchangeDeclare(exchangeProperties.getName(), exchangeProperties.getType(), exchangeProperties.isDurable());
            String routingKey = exchangeProperties.getRoutingKey() == null ? "" : exchangeProperties.getRoutingKey();
            channel.queueBind(Constants.RABBITMQ_FORWARD_QUEUE, exchangeProperties.getName(), routingKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Channel queueDeclareAndBindMultiExchange(List<ExchangeProperties> exchangePropertiesList){
        Channel channel = createChannel();
        try {
            channel.queueDeclare(Constants.RABBITMQ_FORWARD_QUEUE, true, false, false, null);
            exchangePropertiesList.forEach(exchangeProperties -> exchangeDeclareAndBindQueue(channel, exchangeProperties));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return channel;
    }

}
