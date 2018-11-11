package com.creditease.dbus.rabbitmq.forward.domain;

public class ExchangeProperties {
    private String name;
    private String type;
    private String routingKey;
    private boolean durable;
    private boolean autoDelete;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    @Override
    public String toString() {
        return "ExchangeProperties{" +
                "name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", routingKey='" + routingKey + '\'' +
                ", durable=" + durable +
                ", autoDelete=" + autoDelete +
                '}';
    }
}
