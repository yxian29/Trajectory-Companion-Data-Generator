package tcdatagen.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyFileParser {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyFileParser.class);

    private Properties props = new Properties();
    private String propFileName;

    public PropertyFileParser(String propFileName) {
        this.propFileName = propFileName;
    }

    public String getPropFileName() {
        return this.propFileName;
    }

    public void setPropFileName(String filename) {
        this.propFileName = filename;
    }

    public String getProperty(String key) {
        return props.get(key).toString();
    }

    public void parseFile() throws Exception {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);

        try {
            if(inputStream != null) {
                props.load(inputStream);
            } else {
                throw new IOException("failed to load property file");
            }
        } catch (IOException ex) {
            LOG.error(ex.getMessage());
            ex.printStackTrace();
            try {
                File file = new File(propFileName);
                inputStream = new FileInputStream(file.getAbsolutePath());
                props.load(inputStream);
            } catch (IOException ex1) {
                LOG.error("failed to load property file");
                ex1.printStackTrace();
                throw ex1;
            }
        }
    }

}
