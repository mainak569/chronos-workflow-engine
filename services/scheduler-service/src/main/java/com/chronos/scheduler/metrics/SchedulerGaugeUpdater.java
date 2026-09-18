package com.chronos.scheduler.metrics;

import com.chronos.scheduler.domain.ExecutionStatus;
import com.chronos.scheduler.leader.LeaderElectionService;
import com.chronos.scheduler.outbox.OutboxMessageRepository;
import com.chronos.scheduler.outbox.OutboxStatus;
import com.chronos.scheduler.repository.WorkflowExecutionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Refreshes the gauge values periodically, so a Prometheus scrape never queries MongoDB.
 */
@Component
public class SchedulerGaugeUpdater {

    private static final Logger log = LoggerFactory.getLogger(SchedulerGaugeUpdater.class);

    private final SchedulerMetrics metrics;
    private final WorkflowExecutionRepository executionRepository;
    private final OutboxMessageRepository outboxMessageRepository;
    private final LeaderElectionService leaderElectionService;

    public SchedulerGaugeUpdater(SchedulerMetrics metrics,
                                 WorkflowExecutionRepository executionRepository,
                                 OutboxMessageRepository outboxMessageRepository,
                                 LeaderElectionService leaderElectionService) {
        this.metrics = metrics;
        this.executionRepository = executionRepository;
        this.outboxMessageRepository = outboxMessageRepository;
        this.leaderElectionService = leaderElectionService;
    }

    @Scheduled(fixedDelayString = "${chronos.metrics.gauge-refresh-interval:15000}")
    public void refresh() {
        try {
            metrics.updateLeader(leaderElectionService.isLeader());
            metrics.updateActiveExecutions(executionRepository.countByStatusIn(
                    List.of(ExecutionStatus.PENDING, ExecutionStatus.RUNNING)));
            metrics.updateOutboxPending(outboxMessageRepository.countByStatus(OutboxStatus.PENDING));
        } catch (Exception e) {
            log.debug("Could not refresh scheduler gauges: {}", e.getMessage());
        }
    }
}
