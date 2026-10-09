import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An open-model load generator, as a single source file.
 *
 * <p>Open model means a fixed arrival rate: a request is started every
 * {@code 1/rate} seconds whether or not the previous ones have finished. That is
 * the only model that can show a stack queueing. A closed model - N workers in a
 * loop - cannot, because when the server slows down the client sends less, and
 * the latency of a saturated server looks like the latency of an idle one.
 *
 * <p>It is a single file run with {@code java LoadGenerator.java} rather than a
 * Gradle plugin on purpose: a plugin that forks its own JVM uses the Gradle
 * daemon's Java, which on this machine is older than the toolchain, and a load
 * generator has no business deciding which JDK the project compiles against.
 *
 * <p>Each arrival gets a virtual thread, so the generator itself does not become
 * the bottleneck it is trying to measure.
 *
 * <pre>
 * java LoadGenerator.java &lt;url&gt; &lt;ratePerSecond&gt; &lt;durationSeconds&gt; [label]
 * </pre>
 */
public class LoadGenerator {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: LoadGenerator <url> <ratePerSecond> <durationSeconds> [label]");
            System.exit(2);
        }
        String url = args[0];
        double rate = Double.parseDouble(args[1]);
        int durationSeconds = Integer.parseInt(args[2]);
        String label = args.length > 3 ? args[3] : url;

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .executor(Executors.newVirtualThreadPerTaskExecutor())
                .build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(120))
                .GET()
                .build();

        List<Long> latenciesMs = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();

        long intervalNanos = (long) (1_000_000_000L / rate);
        long deadline = System.nanoTime() + durationSeconds * 1_000_000_000L;
        ExecutorService arrivals = Executors.newVirtualThreadPerTaskExecutor();

        long next = System.nanoTime();
        int started = 0;
        while (next < deadline) {
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                TimeUnit.NANOSECONDS.sleep(sleep);
            }
            arrivals.submit(() -> {
                long begin = System.nanoTime();
                try {
                    HttpResponse<Void> response =
                            client.send(request, HttpResponse.BodyHandlers.discarding());
                    latenciesMs.add((System.nanoTime() - begin) / 1_000_000);
                    if (response.statusCode() < 400) {
                        ok.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                } catch (Exception e) {
                    latenciesMs.add((System.nanoTime() - begin) / 1_000_000);
                    failed.incrementAndGet();
                }
            });
            started++;
            next += intervalNanos;
        }

        arrivals.shutdown();
        // Generous: when the server is saturated the tail is the measurement,
        // and cutting it short would report a latency the server did not have.
        arrivals.awaitTermination(5, TimeUnit.MINUTES);

        List<Long> sorted = new ArrayList<>(latenciesMs);
        Collections.sort(sorted);
        System.out.printf("%n== %s%n", label);
        System.out.printf("  arrivals  %d at %.0f/s for %ds%n", started, rate, durationSeconds);
        System.out.printf("  ok %d  failed %d%n", ok.get(), failed.get());
        if (!sorted.isEmpty()) {
            System.out.printf("  median %d ms   p95 %d ms   p99 %d ms   max %d ms%n",
                    percentile(sorted, 50), percentile(sorted, 95),
                    percentile(sorted, 99), sorted.get(sorted.size() - 1));
        }
    }

    private static long percentile(List<Long> sorted, int p) {
        int index = (int) Math.ceil(p / 100.0 * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }
}
