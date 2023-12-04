package com.polykhel.redispriorityqueuepoc;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
public class Event implements Comparable<Event>, Serializable {

    private String eventId;

    private String priority;

    private String status;

    public Event(String eventId, String priority) {
        this.eventId = eventId;
        this.priority = priority;
        this.status = "PENDING";
    }

    @Override
    public int compareTo(Event o) {
        return priority.compareTo(o.getPriority());
    }
}
