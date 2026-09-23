package com.github.highcumontoa.reliableasynceventsjava.demo;

public record OrderResult(long orderId, String requestId, String product, int quantity,
                          long eventId, boolean duplicated) {
}
