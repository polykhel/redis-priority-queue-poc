package com.polykhel.redispriorityqueuepoc;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/queue")
public class QueueController {

    private final PriorityQueueService priorityQueueService;

    public QueueController(PriorityQueueService priorityQueueService) {
        this.priorityQueueService = priorityQueueService;
    }

    @PostMapping("/enqueue")
    public ResponseEntity<String> enqueue(@RequestBody Event event) {
        priorityQueueService.enqueue(event);
        return ResponseEntity.ok("Item enqueued successfully");
    }
}