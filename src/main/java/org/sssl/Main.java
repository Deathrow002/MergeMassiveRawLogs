package org.sssl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main {
    public static void main(String[] args) {
        Properties prop = new Properties();
        try (InputStream input = Main.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find application.properties");
                return;
            }
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            return;
        }

        String inputPathStr = prop.getProperty("input.path");
        String outputPathStr = prop.getProperty("output.path");

        if (inputPathStr == null || outputPathStr == null) {
            System.err.println("Please specify input.path and output.path in application.properties");
            return;
        }

        Path inputPath = Paths.get(inputPathStr);
        Path outputPath = Paths.get(outputPathStr);

        if (!Files.exists(inputPath) || !Files.isDirectory(inputPath)) {
            System.err.println("Input path does not exist or is not a directory: " + inputPath.toAbsolutePath());
            // Create directory just in case for testing purposes? No, user said 'from folder in path', implies it exists.
            // But let's create it on the fly if it doesn't exist to avoid crashing on first run if empty.
            try {
                Files.createDirectories(inputPath);
                System.out.println("Created input directory: " + inputPath.toAbsolutePath());
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
        }

        // Create output directory parent if needed
        try {
            if (outputPath.getParent() != null) {
                Files.createDirectories(outputPath.getParent());
            }
        } catch (IOException e) {
            System.err.println("Failed to create output directory: " + e.getMessage());
            return;
        }

        System.out.println("Reading files from: " + inputPath.toAbsolutePath());
        System.out.println("Writing to: " + outputPath.toAbsolutePath());

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
             Stream<Path> stream = Files.list(inputPath)) {

            List<Path> files = stream
                    .filter(file -> !Files.isDirectory(file))
                    .filter(file -> file.toString().toLowerCase().endsWith(".txt"))
                    .filter(file -> !file.toAbsolutePath().equals(outputPath.toAbsolutePath()))
                    .sorted()
                    .collect(Collectors.toList());

            if (files.isEmpty()) {
                System.out.println("No .txt files found in input directory.");
            } else {
                AtomicInteger processedCount = new AtomicInteger(0);
                int totalFiles = files.size();

                // Use Virtual Threads (Java 21+) for massive concurrency
                // ReentrantLock is preferred over synchronized for virtual threads to avoid pinning
                ReentrantLock lock = new ReentrantLock();
                
                ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
                try {
                    System.out.println("Using virtual threads for merging " + totalFiles + " files...");

                    for (Path file : files) {
                        executor.submit(() -> {
                            System.out.println("Merging file: " + file.getFileName());
                            try (Stream<String> lines = Files.lines(file, StandardCharsets.UTF_8)) {
                                lock.lock();
                                try {
                                    lines.forEach(line -> {
                                        try {
                                            writer.write(line);
                                            writer.newLine();
                                        } catch (IOException e) {
                                            throw new RuntimeException("Error writing to output file", e);
                                        }
                                    });
                                } finally {
                                    lock.unlock();
                                }
                                int current = processedCount.incrementAndGet();
                                if (current % 100 == 0) {
                                    System.out.println("Processed " + current + " of " + totalFiles + " files.");
                                }
                            } catch (IOException e) {
                                System.err.println("Error reading file: " + file + " - " + e.getMessage());
                            } catch (RuntimeException e) {
                                if (e.getCause() instanceof IOException) {
                                    System.err.println("Error writing output for file: " + file + " - " + e.getCause().getMessage());
                                } else {
                                    throw e;
                                }
                            }
                        });
                    }
                } finally {
                    executor.shutdown();
                    try {
                        if (!executor.awaitTermination(24, TimeUnit.HOURS)) {
                            executor.shutdownNow();
                        }
                    } catch (InterruptedException e) {
                        executor.shutdownNow();
                        Thread.currentThread().interrupt();
                    }
                }

                System.out.println("Merge completed successfully. Added content from " + totalFiles + " files.");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}