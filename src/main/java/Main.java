import com.rabbitmq.client.*;
import definitions.SimulationEventType;

import java.io.IOException;
import java.util.EnumMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static definitions.SimulationEventType.*;

public class Main {

    private static final ExecutorService consumerThread = Executors.newSingleThreadExecutor();
    private static final EnumMap<SimulationEventType, Integer> queueStatistics = new EnumMap<>(SimulationEventType.class);
    private static Channel channel;

    private static int availableFood = 0;
    private static int currentPopulationCount = 0;
    private static int initialPopulationCount = 0;
    private static float populationGrowth;
    private static boolean computedInitialPopulation = false;

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

    private static void computeFood(){
        availableFood = queueStatistics.get(FOOD_CREATED) * 5 - queueStatistics.get(FOOD_REDUCED);
    }

    private static void computePopulation() {
        currentPopulationCount = queueStatistics.get(BIRTH) +
                queueStatistics.get(DEATH_AGE) -
                queueStatistics.get(DEATH_STARVATION);

        if (initialPopulationCount != 0)
            populationGrowth = (float) currentPopulationCount / initialPopulationCount ;

        if (computedInitialPopulation)
            return;

        initialPopulationCount = queueStatistics.get(BIRTH) - queueStatistics.get(REPRODUCTION)/2;
        computedInitialPopulation = true;
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
        computePopulation();
        computeFood();

    }

    private static void printCounts(){
        System.out.println("Queue Statistics:");
        for (EnumMap.Entry<SimulationEventType, Integer> entry : queueStatistics.entrySet()) {
            System.out.println("    " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();
        System.out.println("    " + "Initial population" + ": " + initialPopulationCount);
        System.out.println("    " + "Current population" + ": " + currentPopulationCount);
        System.out.println("    " + "Population growth " + ": " + String.format("%.0f%%",populationGrowth*100));
        System.out.println("    " + "Available food" + ": " + availableFood);
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
