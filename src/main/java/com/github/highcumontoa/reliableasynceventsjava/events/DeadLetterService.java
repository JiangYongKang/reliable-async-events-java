package com.github.highcumontoa.reliableasynceventsjava.events;

import java.util.List;

/** 死信查询与恢复。 */
public interface DeadLetterService {

    List<EventRecord> list();

    EventRecord replay(String eventId);
}
