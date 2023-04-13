package org.opensearch;

import org.junit.jupiter.api.Test;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.collect.Map;
import org.opensearch.common.settings.Settings;

public class IndexShardTest {
  @Test
  public void t1() {
    IndexMetadata indexMetadata = IndexMetadata.builder("my-index")
        .settings(Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .build())
        .putMapping(new MappingMetadata("_doc", Map.of("name", Map.of("type", "text"))))
        .build();
  }
}
