package gr.imsi.athenarc.visual.middleware.experiments.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtil {

    private static void buildDirectory(String filePath) throws IOException {
        Path pathDir = new File(filePath).toPath();
        Files.createDirectory(pathDir);
    }

    public static void build(String filePath) throws IOException {
        if (!(new File(filePath).exists())) buildDirectory(filePath);
    }
}
