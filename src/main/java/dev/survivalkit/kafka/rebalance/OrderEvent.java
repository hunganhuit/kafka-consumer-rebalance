package dev.survivalkit.kafka.rebalance;

/**
 * The business payload flowing through the topic. Kept trivial on purpose — the
 * chapter is about the consumer lifecycle, not about modeling orders.
 *
 * @param orderId a stable id we can use to detect lost or duplicated processing
 */
public record OrderEvent(String orderId) {}
