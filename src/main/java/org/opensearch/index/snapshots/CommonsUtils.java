package org.opensearch.index.snapshots;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;

public class CommonsUtils {
  private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY =
      new NamedXContentRegistry(
          ClusterModule.getNamedXWriteables()
      );

  public static MapperService mapperService(String index) {
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/penghuo/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/penghuo/tmp/opensearch/data")
        .build();
    IndexSettings indexSettings = newIndexSettings(index, settings);

    Map<String, NamedAnalyzer> analyzers = new HashMap<>();
    analyzers.put(
        AnalysisRegistry.DEFAULT_ANALYZER_NAME,
        new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())
    );

    return new MapperService(
        indexSettings,
        new IndexAnalyzers(analyzers, Collections.emptyMap(), Collections.emptyMap()),
        xContentRegistry(),
        new SimilarityService(indexSettings, null, Collections.emptyMap()),
        new IndicesModule(Collections.emptyList()).getMapperRegistry(),
        () -> null,
        () -> false,
        null
    );
  }

  public static IndexSettings newIndexSettings(String index, Settings settings, Setting<?>... setting) {
    return newIndexSettings(
        new Index(index, settings.get(IndexMetadata.SETTING_INDEX_UUID, IndexMetadata.INDEX_UUID_NA_VALUE)),
        settings,
        setting
    );
  }

  public static IndexSettings newIndexSettings(Index index, Settings settings, Setting<?>... setting) {
    return newIndexSettings(index, settings, Settings.EMPTY, setting);
  }

  public static IndexSettings newIndexSettings(Index index, Settings indexSetting, Settings nodeSettings, Setting<?>... setting) {
    Settings build = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
        .put(indexSetting)
        .build();
    IndexMetadata metadata = IndexMetadata.builder(index.getName()).settings(build).build();
    Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
    if (setting.length > 0) {
      settingSet.addAll(Arrays.asList(setting));
    }
    return new IndexSettings(metadata, nodeSettings, new IndexScopedSettings(Settings.EMPTY, settingSet));
  }

  protected static NamedXContentRegistry xContentRegistry() {
    return DEFAULT_NAMED_X_CONTENT_REGISTRY;
  }
}
