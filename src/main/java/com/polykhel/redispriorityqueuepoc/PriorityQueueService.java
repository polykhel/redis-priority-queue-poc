package com.polykhel.redispriorityqueuepoc;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RPriorityQueue;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class PriorityQueueService {

    private static final String QUEUE_NAME = "main_queue";
    public static final Random RANDOM = new Random();

    @Value("${spring.application.name}")
    private String appName;

    private final RedissonClient redissonClient;

    public PriorityQueueService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    // priority is determined by the Comparable/Comparator

    @Scheduled(fixedDelay = 500)
    public void publisher() {
        enqueueWithPriority(new Event(UUID.randomUUID().toString(), String.valueOf(System.currentTimeMillis())));
    }

    public void enqueueWithPriority(Event item) {
        RPriorityQueue<Event> priorityQueue = redissonClient.getPriorityQueue(QUEUE_NAME);
        priorityQueue.add(item);
    }

    @Scheduled(fixedDelay = 1000)
    public void dequeueWithLock() throws InterruptedException {
        RPriorityQueue<Event> priorityQueue = redissonClient.getPriorityQueue(QUEUE_NAME);

        Event event = priorityQueue.poll();
        if (event != null) {
            log.info("{} trying to consume {}", appName, event.getEventId());
            RLock lock = redissonClient.getLock("eventLock:" + event.getEventId());

            // RLock has a lock watchdog that extends the expiration while lock holder Redisson instance is alive
            // By default lock watchdog timeout is 30 seconds and can be changed through Config.lockWatchdogTimeout setting.
            if (lock.tryLock()) {
                try {
                    log.info("{} acquired lock for {}", appName, event.getEventId());
                    event.setStatus("RUNNING");
                    // Process the dequeued event
                    processEvent(event);
                    event.setStatus("DONE");
                    // if failure is encountered, should it go back to the queue with the highest priority or discard it
                } finally {
                    lock.unlock();
                }
            } else {
                log.warn("{} failed to acquired lock for {}", appName, event.getEventId());
            }
        }
    }

    private void processEvent(Event event) throws InterruptedException {
        // process event
        Thread.sleep(RANDOM.nextInt(9000));
        log.info("{} processed event {} ", appName, event.getEventId());
    }
}
