package com.polykhel.redispriorityqueuepoc;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/queue")
public class QueueController {

    private PriorityQueueService priorityQueueService;

    public QueueController(PriorityQueueService priorityQueueService) {
        this.priorityQueueService = priorityQueueService;
    }

    @PostMapping("/enqueue")
    public ResponseEntity<String> enqueue(@RequestParam String item, @RequestParam String priority) {
        priorityQueueService.enqueueWithPriority(new Event(item, priority));
        return ResponseEntity.ok("Item enqueued successfully");
    }
}