package com.chronos.gateway.filter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory fixed-window rate limiter (per gateway instance).
 */
public class RateLimiter {

    private static final long WINDOW_MS = 60_000;

    private final int limitPerWindow;
    private final Map<String, Window> windows = new ConcurrentHashMap<>();

    public RateLimiter(int limitPerWindow) {
        this.limitPerWindow = limitPerWindow;
    }

    /**
     * Record a request for the client.
     *
     * @return true if the request is within the limit
     */
    public boolean tryAcquire(String clientKey, long nowMs) {
        long windowStart = nowMs - (nowMs % WINDOW_MS);
        Window window = windows.compute(clientKey, (key, current) ->
                current == null || current.start != windowStart ? new Window(windowStart) : current);
        boolean allowed = window.count.incrementAndGet() <= limitPerWindow;

        // Drop windows of idle clients so the map does not grow without bound
        if (windows.size() > 10_000) {
            windows.values().removeIf(w -> w.start < windowStart);
        }
        return allowed;
    }

    /**
     * Seconds until the current window ends.
     */
    public long retryAfterSeconds(long nowMs) {
        return Math.max(1, (WINDOW_MS - (nowMs % WINDOW_MS)) / 1000);
    }

    private static final class Window {
        private final long start;
        private final AtomicInteger count = new AtomicInteger();

        private Window(long start) {
            this.start = start;
        }
    }
}
