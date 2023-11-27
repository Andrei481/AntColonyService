import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.EnumMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Main {

    private static final ExecutorService consumerThread = Executors.newSingleThreadExecutor();
    private static final EnumMap<SimulationEventType, Integer> queueStatistics = new EnumMap<>(SimulationEventType.class);
    private static Channel channel;

    public static void main(String[] args) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            Connection connection = factory.newConnection();
            channel = connection.createChannel();

            /* Initialise enum map: each queue starts with 0 messages */
            for (SimulationEventType eventType : SimulationEventType.values()) {
                queueStatistics.put(eventType, 0);
            }

            startLoop();

        } catch (IOException | TimeoutException e) {
            System.err.println("Error connecting to RabbitMQ: " + e.getMessage());
        }
    }

    private static void startLoop() {
        consumerThread.execute(() -> {
            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    updateCounts();
                    printCounts();

                } catch (InterruptedException e) {
                    System.err.println("Thread interrupted: " + e.getMessage());
                }
            }
        });
    }

    private static void updateCounts(){
        /* Check each queue and update message counts */
        queueStatistics.replaceAll((q, v) -> getMessageCount(q));
    }

    private static void printCounts(){
        System.out.println("Queue Statistics:");
        for (EnumMap.Entry<SimulationEventType, Integer> entry : queueStatistics.entrySet()) {
            System.out.println("    " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();
    }

    private static int getMessageCount(SimulationEventType queueName) {
        try {
            AMQP.Queue.DeclareOk result = channel.queueDeclare(queueName.name(), false, false, false, null);
            return result.getMessageCount();
            // TO DO: build own counter

        } catch (IOException e) {
            System.err.println("Error checking message count for queue " + queueName + ": " + e.getMessage());
            return -1;
        }
    }


}
