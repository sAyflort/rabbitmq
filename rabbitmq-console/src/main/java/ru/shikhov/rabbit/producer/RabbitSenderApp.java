package ru.shikhov.rabbit.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;


public class RabbitSenderApp {
    private static final String EXCHANGE_NAME = "ITBlog";
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            while(true) {
                String str = scanner.nextLine();
                String topicOrExit = str.split(" ", 2)[0];
                String msg;
                if (topicOrExit.equals("exit")) return;
                if (str.split(" ", 2)[1] == null) {
                    continue;
                } else {
                    msg = str.split(" ", 2)[1];
                }
                channel.basicPublish(EXCHANGE_NAME, topicOrExit, null, msg.getBytes("UTF-8"));
                System.out.println("[*] В тему " + topicOrExit + " отправлено: " + msg);
            }
        }
    }
}