package io.transwarp.util;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class Read {
    private Logger logger = Logger.getLogger(this.getClass());

    public Read() {
        logger.info("Loading " + this.getClass().getName() + " constructor successfully");
    }

    public String readAsString(String path) {
        String encoding = "UTF-8";
        File file = new File(path);
        Long fileLen = file.length();
        byte[] fileContent = new byte[fileLen.intValue()];
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            fis.read(fileContent);
            return new String(fileContent, encoding);
        } catch (UnsupportedEncodingException uee) {
            logger.error("The OS does not support " + encoding, uee);
            return null;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public List<String> readLinesAsList(String path) {
        try {
            return Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
        } catch (UnsupportedEncodingException uee) {
            logger.error("The OS does not support " + StandardCharsets.UTF_8, uee);
            return null;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return null;
        }
    }

}
