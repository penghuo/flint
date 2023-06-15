package org.opensearch.snapshot;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

public class Snapshot {

  public void testSnapshotRead() throws IOException {
    readSnapshot("/Users/penghuo/tmp/opensearch/snapshot/indices/phjcemrBT0SRFwbcXtbjyw/0/index" +
        "-U_ZmpkuaS2i3vrM5nDY0yQ");
  }

  public void readSnapshot(String snapshotName) throws IOException {
    InputStream blob = readBlob(snapshotName);
    XContentParser parser = XContentType.JSON.xContent()
        .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, blob);
    System.out.println(parser.text());
  }

  public InputStream readBlob(String name) throws IOException {
    final Path resolvedPath = Paths.get(name);
    try {
      return Files.newInputStream(resolvedPath);
    } catch (FileNotFoundException fnfe) {
      throw new NoSuchFileException("[" + name + "] blob not found");
    }
  }
}
