import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.IntStream;

/**
 * Recursively crawls sites to a given depth, downloading documents and following links in them
 *
 * @author Salakhov Kamil
 */

public class WebCrawler implements AdvancedCrawler {
    private final int perHost;
    private final Downloader downloader;
    private final ExecutorService downloadService;
    private final ExecutorService extractorService;
    private final HostGetter hostGetter;

    /**
     * Single WebCrawler constructor
     *
     * @param downloader       to download page
     * @param downloadService  allowed number of threads to download pages
     * @param extractorService allowed number of threads to extract links from pages
     * @param perHost          allowed number of threads to download pages from the same host.
     * @param hostGetter       to get host of url
     */
    public WebCrawler(Downloader downloader, int downloadService, int extractorService,
                      int perHost, HostGetter hostGetter) {
        this.downloader = downloader;
        this.downloadService = Executors.newFixedThreadPool(downloadService);
        this.extractorService = Executors.newFixedThreadPool(extractorService);
        this.perHost = perHost;
        this.hostGetter = hostGetter;
    }

    @Override
    public Result download(String url, int depth) {
        return new WebDownloader().download(url, depth, new NullSet(null));
    }

    @Override
    public Result download(String url, int depth, List<String> hosts) {
        return new WebDownloader().download(url, depth, new NullSet(hosts.stream().distinct().toList()));
    }

    @Override
    public void close() {
        shutdownAndAwaitTermination(downloadService);
        shutdownAndAwaitTermination(extractorService);
    }

    private static final int AWAIT_TIME = 50;

    private void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                if (!pool.awaitTermination(AWAIT_TIME, TimeUnit.SECONDS)) {
                    System.err.println("Pool did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private final class WebDownloader {
        private final Queue<String> downloaded = new LinkedBlockingQueue<>();
        private final Map<String, IOException> errors = new ConcurrentHashMap<>();
        private final Set<String> visitedUrls = ConcurrentHashMap.newKeySet();
        private final Map<String, HostHandler> hosts = new ConcurrentHashMap<>();

        private Result download(String rootUrl, int depth, NullSet allowedHosts) {
            Queue<String> sameHeightUrls = new LinkedBlockingQueue<>();
            Queue<String> upHeightUrls = new LinkedBlockingQueue<>();

            sameHeightUrls.add(rootUrl);

            IntStream.rangeClosed(1, depth).forEach(i -> {
                Phaser phaser = new Phaser(1);
                sameHeightUrls.stream().filter(visitedUrls::add).forEach(url -> {
                    try {
                        HostHandler hostHandler = getHostHandler(url, allowedHosts);
                        if (!Objects.isNull(hostHandler)) {
                            phaser.register();

                            hostHandler.addTask(
                                    () -> submitToExtractor(url, upHeightUrls, phaser,
                                            hostHandler, i != depth)
                            );
                        }
                    } catch (IOException e) {
                        errors.put(url, e);
                    }
                });

                phaser.arriveAndAwaitAdvance();
                moveContent(upHeightUrls, sameHeightUrls);
            });

            return new Result(downloaded.stream().toList(), errors);
        }

        private HostHandler getHostHandler(String url, NullSet allowedHosts) throws MalformedURLException {
            String host = hostGetter.getHost(url);

            if (!allowedHosts.contains(host)) {
                return null;
            }

            return hosts.computeIfAbsent(host, h -> new HostHandler());
        }

        private void submitToExtractor(String url, Queue<String> urls, Phaser phaser,
                                       HostHandler hostHandler, boolean needToAddChildren) {
            try {
                Document document = downloader.download(url);
                downloaded.add(url);

                if (needToAddChildren) {
                    phaser.register();

                    extractorService.submit(() -> newExtractorTask(url, urls, phaser, document));
                }
            } catch (IOException e) {
                errors.put(url, e);
            } finally {
                phaser.arrive();
                hostHandler.runTask();
            }
        }

        private void newExtractorTask(String url, Queue<String> urls, Phaser phaser, Document document) {
            try {
                urls.addAll(document.extractLinks());
            } catch (IOException e) {
                errors.put(url, e);
            } finally {
                phaser.arrive();
            }
        }

        private static <T> void moveContent(Queue<T> from, Queue<T> to) {
            to.clear();
            to.addAll(from);
            from.clear();
        }

        private class HostHandler {
            private int loading;
            private final Queue<Runnable> waiting = new ArrayDeque<>();

            public synchronized void runTask() {
                if (waiting.isEmpty()) {
                    loading--;
                } else {
                    downloadService.submit(waiting.poll());
                }
            }

            public synchronized void addTask(Runnable task) {
                if (loading >= perHost) {
                    waiting.add(task);
                } else {
                    downloadService.submit(task);
                    loading++;
                }
            }
        }
    }

    private static class NullSet {
        private final Set<String> set;

        /**
         * Constructor from list
         *
         * @param list on which will work contains
         */
        public NullSet(List<String> list) {
            this.set = Objects.isNull(list) ? null : new HashSet<>(list);
        }

        /**
         * @param str String to check if contains
         * @return If set is null, then return always true, otherwise return set.contains(str)
         */
        public boolean contains(String str) {
            return Objects.isNull(set) || set.contains(str);
        }
    }
}