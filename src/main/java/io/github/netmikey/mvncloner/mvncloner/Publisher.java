package io.github.netmikey.mvncloner.mvncloner;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Publishes from the mirror directory into a remote target maven repository.
 * 
 * @author mike
 */
@Component
public class Publisher {

    private static final Logger LOG = LoggerFactory.getLogger(Publisher.class);

    @Value("${target.root-url}")
    private String rootUrl;

    @Value("${target.user:#{null}}")
    private String username;

    @Value("${target.password:#{null}}")
    private String password;

    @Value("${mirror-path:./mirror/}")
    private String rootMirrorPath;

    @Value("${target.publisher-threads:10}")
    private Integer publisherThreads;

    public void publish() throws Exception {
        LOG.info("Publishing to " + rootUrl + " ...");
        ThreadPoolExecutor requestThreadPool = (ThreadPoolExecutor)Executors.newFixedThreadPool(this.publisherThreads);
        publishDirectory(requestThreadPool, rootUrl, Paths.get(rootMirrorPath).normalize());
        try {
            requestThreadPool.shutdown();
            requestThreadPool.awaitTermination(600L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
        }
        LOG.info("Publishing complete.");
    }

    public void publishDirectory(ThreadPoolExecutor threadPoolExecutor, String repositoryUrl, Path mirrorPath)
        throws IOException, InterruptedException {

        LOG.debug("Switching to mirror directory: " + mirrorPath.toAbsolutePath());

        List<Path> recursePaths = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(mirrorPath)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    recursePaths.add(path);
                } else {
                    threadPoolExecutor.execute(
                            new RunnableUploader(repositoryUrl, this.username, this.password, path)
                    );
                }
            }
        }

        // Tail recursion
        for (Path recursePath : recursePaths) {
            String subpath = mirrorPath.relativize(recursePath).toString();
            publishDirectory(threadPoolExecutor, appendUrlPathSegment(repositoryUrl, subpath), recursePath);
        }
    }

    private String appendUrlPathSegment(String baseUrl, String segment) {
        StringBuffer result = new StringBuffer(baseUrl);

        if (!baseUrl.endsWith("/")) {
            result.append('/');
        }
        result.append(segment);
        result.append('/');

        return result.toString();
    }

    private class RunnableUploader implements Runnable {
        private String repositoryUrl = null;
        private String username = null;
        private String password = null;
        private Path targetFile = null;

        public RunnableUploader(String repositoryUrl, String username, String password, Path targetFile) {
            this.repositoryUrl = repositoryUrl;
            this.username = username;
            this.password = password;
            this.targetFile = targetFile;
        }

        public void run() {
            try {
                String filename = this.targetFile.getFileName().toString();
                String targetUrl = this.repositoryUrl + filename;
                LOG.info("Uploading " + targetUrl);
                HttpClient httpClient = HttpClient.newBuilder()
                        .version(HttpClient.Version.HTTP_1_1)
                        .followRedirects(HttpClient.Redirect.NORMAL)
                        .build();
                HttpRequest request = Utils.setCredentials(HttpRequest.newBuilder(), this.username, this.password)
                        .uri(URI.create(targetUrl))
                        .timeout(Duration.ofMinutes(10))    // for big files... just in case
                        .header("X-Checksum-Sha1", Utils.computeFileSHA1(this.targetFile.toFile()))
                        .PUT(BodyPublishers.ofFile(this.targetFile))
                        .build();
                HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
                int statusCode = response.statusCode();
                if (statusCode >= 200 && statusCode <= 299) {
                    LOG.info("Uploaded Successfully!");
                } else if (statusCode == 403) {
                    LOG.info("Already uploaded");
                } else {
                    LOG.error("Something bad happened during upload: " + statusCode);
                    LOG.error("Error message body: " + response.body());
                }
                LOG.debug("statusCode: " + statusCode);
                LOG.debug("message body: " + response.body());
            } catch (IOException | InterruptedException exception) {
                LOG.error(exception.getMessage());
            }
        }
    }
}
