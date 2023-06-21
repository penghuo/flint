package org.opensearch.respositories;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import org.opensearch.common.UUIDs;

public class ShardIdTranslog {

  public static void writeShardId(String sourceFilePath, int shardId) throws IOException {
    if (!Files.exists(Path.of(sourceFilePath))) {
      Files.createFile(Path.of(sourceFilePath));
    }

    String tempFilePath = sourceFilePath + "." + UUIDs.base64UUID();

    Files.copy(Path.of(sourceFilePath), Path.of(tempFilePath), StandardCopyOption.REPLACE_EXISTING);

    // Append content to the target file
    Files.write(Path.of(tempFilePath), String.format("%d\n", shardId).getBytes(),
        java.nio.file.StandardOpenOption.APPEND);

    Files.move(Path.of(tempFilePath), Path.of(sourceFilePath), StandardCopyOption.REPLACE_EXISTING);
  }

}
