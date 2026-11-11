package cn.itsub.game.rpc.demo.action;

import cn.itsub.game.rpc.demo.message.RpcDemoRequest;
import cn.itsub.kynet.framework.action.ActionContext;
import cn.itsub.kynet.framework.action.annotation.ActionController;
import cn.itsub.kynet.framework.action.annotation.ActionMethod;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@ActionController(102)
public class OneWayBenchmarkAction {
    private static final Duration MEASURE_WINDOW = Duration.ofSeconds(3);

    private static final AtomicLong totalReceived = new AtomicLong();
    private static final AtomicLong measuredReceived = new AtomicLong();
    private static final AtomicLong measureStartNanos = new AtomicLong();
    private static final AtomicLong measureEndNanos = new AtomicLong();
    private static final AtomicBoolean reported = new AtomicBoolean();

    @ActionMethod(1)
    public void receive(RpcDemoRequest request, ActionContext context) {
        totalReceived.incrementAndGet();

        long now = System.nanoTime();
        long start = measureStartNanos.get();
        if (start == 0L && measureStartNanos.compareAndSet(0L, now)) {
            start = now;
            measureEndNanos.set(now + MEASURE_WINDOW.toNanos());
            log.info("one-way receiver measurement started, sessionId={}, userId={}, payload={}",
                    context.getSessionId(), context.getUserId(), request == null ? null : request.getMessage());
        } else if (start == 0L) {
            start = measureStartNanos.get();
        }

        long end = measureEndNanos.get();
        if (start == 0L || end == 0L) {
            return;
        }

        if (start != 0L && now <= end) {
            measuredReceived.incrementAndGet();
        }

        if (start != 0L && now >= end && reported.compareAndSet(false, true)) {
            long measured = measuredReceived.get();
            double elapsedSeconds = (end - start) / 1_000_000_000.0;
            double qps = elapsedSeconds > 0 ? measured / elapsedSeconds : 0;
            log.info("one-way receiver qps={}, measuredReceived={}, totalReceived={}, measureWindowSec={}",
                    String.format("%.2f", qps),
                    measured,
                    totalReceived.get(),
                    String.format("%.2f", elapsedSeconds));
        }
    }
}
