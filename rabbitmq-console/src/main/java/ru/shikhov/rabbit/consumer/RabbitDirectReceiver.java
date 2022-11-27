package ru.shikhov.rabbit.consumer;

import com.rabbitmq.client.*;

import java.util.Optional;
import java.util.Scanner;

public class RabbitDirectReceiver {
    private static final String EXCHANGE_NAME = "ITBlog";
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();

        String str = scanner.nextLine();
        String command = str.split(" ", 2)[0];
        String msg = str.split(" ", 2)[1];
        if(msg == null) return;
        if(command.equals("set_topic")) {
            channel.queueBind(queueName, EXCHANGE_NAME, msg);
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Получено: '" + message + "'" + " в тему '" + delivery.getEnvelope().getRoutingKey() + "'");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        }

        while (true) {
            str = scanner.nextLine();
            if(str.split(" ")[0].equals("exit")) return;
            if(str.split(" ")[1] == null) {
                continue;
            }
            if(str.split(" ")[0].equals("set_topic")) {
                channel.queueBind(queueName, EXCHANGE_NAME, str.split(" ")[1]);
            }
            if(str.split(" ")[0].equals("delete")) {
                channel.queueUnbind(queueName, EXCHANGE_NAME, str.split(" ")[1]);
            }
        }
    }
}
