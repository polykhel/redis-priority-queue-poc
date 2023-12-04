package com.polykhel.redispriorityqueuepoc;

import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RPriorityQueue;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
public class PriorityQueueService {

    private static final String SHARD_LIST = "main_shards";
    public static final Random RANDOM = new Random();

    @Value("${spring.application.name}")
    private String appName;

    @Value("${organization.weight:1}")
    private int organizationWeight;

    private final RedissonClient redissonClient;

    public PriorityQueueService(RedissonClient redissonClient) {
        this.redissonClient = redissonClient;
    }

    // priority is determined by the Comparable/Comparator

//    @Scheduled(fixedDelay = 500)
    public void publisher() {
        enqueue(new Event(UUID.randomUUID().toString(), appName, organizationWeight, String.valueOf(System.currentTimeMillis())));
    }

    public void enqueue(Event item) {
        RScoredSortedSet<String> shardList = redissonClient.getScoredSortedSet(SHARD_LIST);
        shardList.add(item.getOrganizationWeight(), item.getOrganizationId());

        RPriorityQueue<Event> priorityQueue = redissonClient.getPriorityQueue("queue:" + item.getOrganizationId());
        priorityQueue.add(item);
    }

    @Scheduled(fixedDelay = 1000)
    public void dequeueWithLock() throws InterruptedException {
        RScoredSortedSet<String> shardList = redissonClient.getScoredSortedSet(SHARD_LIST);

        if (shardList.isEmpty()) {
            log.info("{} queue is empty", appName);
            return;
        }

        String shardId = getNextShardId(shardList);
        RPriorityQueue<Event> priorityQueue = redissonClient.getPriorityQueue("queue:" + shardId);

        Event event = priorityQueue.poll();
        if (event != null) {
            log.info("{} trying to consume {} from shard {} organization {}", appName, event.getEventId(), shardId, event.getOrganizationId());
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

    public String getNextShardId(RScoredSortedSet<String> shardList) {
        // remove empty shards

        double totalWeight = shardList.entryRange(0, -1).stream().mapToDouble(ScoredEntry::getScore).sum();

        double randomWeight = RANDOM.nextDouble() * totalWeight;

        for (String shardId : shardList) {
            randomWeight -= shardList.getScore(shardId);
            if (randomWeight <= 0) {
                return shardId;
            }
        }

        // Fallback (should not reach here)
        return shardList.first();
    }

    private void processEvent(Event event) throws InterruptedException {
        // process event
        Thread.sleep(RANDOM.nextInt(9000));
        log.info("{} processed event {} ", appName, event.getEventId());
    }
}
