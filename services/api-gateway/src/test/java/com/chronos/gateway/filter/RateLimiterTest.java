package com.chronos.gateway.filter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterTest {

    private static final long WINDOW_START = 1_800_000_000_000L - (1_800_000_000_000L % 60_000);

    @Test
    void allowsRequestsUpToTheLimitPerWindow() {
        RateLimiter limiter = new RateLimiter(3);

        assertThat(limiter.tryAcquire("user:a", WINDOW_START)).isTrue();
        assertThat(limiter.tryAcquire("user:a", WINDOW_START + 1)).isTrue();
        assertThat(limiter.tryAcquire("user:a", WINDOW_START + 2)).isTrue();
        assertThat(limiter.tryAcquire("user:a", WINDOW_START + 3)).isFalse();
    }

    @Test
    void limitsAreTrackedPerClient() {
        RateLimiter limiter = new RateLimiter(1);

        assertThat(limiter.tryAcquire("user:a", WINDOW_START)).isTrue();
        assertThat(limiter.tryAcquire("user:b", WINDOW_START)).isTrue();
        assertThat(limiter.tryAcquire("user:a", WINDOW_START)).isFalse();
    }

    @Test
    void newWindowResetsTheCount() {
        RateLimiter limiter = new RateLimiter(1);

        assertThat(limiter.tryAcquire("ip:1.2.3.4", WINDOW_START)).isTrue();
        assertThat(limiter.tryAcquire("ip:1.2.3.4", WINDOW_START + 59_999)).isFalse();
        assertThat(limiter.tryAcquire("ip:1.2.3.4", WINDOW_START + 60_000)).isTrue();
    }

    @Test
    void retryAfterPointsToTheEndOfTheWindow() {
        RateLimiter limiter = new RateLimiter(1);

        assertThat(limiter.retryAfterSeconds(WINDOW_START + 45_000)).isEqualTo(15);
        assertThat(limiter.retryAfterSeconds(WINDOW_START + 59_900)).isEqualTo(1);
    }
}
