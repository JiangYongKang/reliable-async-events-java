package com.github.highcumontoa.reliableasynceventsjava.demo;

/**
 * 下单命令（演示业务输入）。
 */
public record OrderCommand(String requestId, String product, int quantity) {
}
