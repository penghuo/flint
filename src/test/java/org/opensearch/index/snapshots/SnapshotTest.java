package org.opensearch.index.snapshots;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.opensearch.Version.V_3_0_0;
import static org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.opensearch.repositories.ShardGenerations.NEW_SHARD_GEN;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.bak.IndexSettingsModule;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.respositories.FsRepository;
import org.opensearch.search.SearchModule;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.DummyShardLock;

public class SnapshotTest extends EngineTestCase {

  private Settings settings() {
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, V_3_0_0)
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/penghuo/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/penghuo/tmp/opensearch/data")
        .put(FsRepository.REPOSITORIES_LOCATION_SETTING.getKey(), "flintsnapshot")
        .build();
    return settings;
  }

  private RepositoryMetadata repositoryMetadata() {
    return new RepositoryMetadata("myRepository", "myTestType", settings());
  }

  private ParsedDocument document(String index, String id, String json, String mapping)
      throws IOException {
    MapperService mapperService = mapperService(index);
    DocumentMapper docMapper =
        mapperService.parse(SINGLE_MAPPING_NAME, new CompressedXContent(mapping));

    byte[] data = json.getBytes();
    BytesArray bytesArray = new BytesArray(data, 0, data.length);
    return docMapper.parse(new SourceToParse(index, id, bytesArray,
        XContentType.JSON));
  }

  private MapperService mapperService(String index) {
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/penghuo/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/penghuo/tmp/opensearch/data")
        .build();
    IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(index, settings);

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

  private void deleteFiles(Path directoryPath) {
    try {
      Files.walk(directoryPath)
          .filter(Files::isRegularFile)
          .forEach(path -> {
            try {
              Files.delete(path);
              System.out.println("Deleted file: " + path);
            } catch (IOException e) {
              System.err.println("Failed to delete file: " + path);
            }
          });
    } catch (IOException e) {
      System.err.println("Failed to traverse directory: " + directoryPath);
    }
  }

  private static String getShardStateId(IndexCommit snapshotIndexCommit) throws IOException {
    final Map<String, String> userCommitData = snapshotIndexCommit.getUserData();
    final SequenceNumbers.CommitInfo seqNumInfo =
        SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userCommitData.entrySet());
    final long maxSeqNo = seqNumInfo.maxSeqNo;
    return userCommitData.get(Engine.HISTORY_UUID_KEY)
        + "-"
        + userCommitData.getOrDefault(Engine.FORCE_MERGE_UUID_KEY, "na")
        + "-"
        + maxSeqNo;
  }

  private String mappings() {
    return "{\n" +
        "  \"dynamic\": false,\n" +
        "  \"properties\": {\n" +
        "    \"aInt\": {\n" +
        "      \"type\": \"integer\"\n" +
        "    },\n" +
        "    \"aString\": {\n" +
        "      \"type\": \"keyword\"\n" +
        "    },\n" +
        "    \"aText\": {\n" +
        "      \"type\": \"text\"\n" +
        "    }\n" +
        "  }\n" +
        "}";
  }

  private ShardId shardId(String indexName) {
    return new ShardId(new Index(indexName, "_na_"), 0);
  }

  private IndexMetadata indexMetadata(String indexName) throws IOException {
    String mappings = "{\"_doc\":{\"dynamic\":false," +
        "\"properties\":{\"aInt\":{\"type\":\"integer\"},\"aString\":{\"type\":\"keyword\"},\"aText\":{\"type\":\"text\"}}}}";
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, V_3_0_0)
        .build();
    return IndexMetadata.builder(indexName)
        .version(V_3_0_0.id)
        .settings(settings)
        .numberOfReplicas(0)
        .numberOfShards(1)
        .putMapping(mappings)
        .state(IndexMetadata.State.OPEN)
        .build();
  }

  private IndexSettings indexSettings(String indexName) throws IOException {
    IndexMetadata metadata = indexMetadata(indexName);
    Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
    return new IndexSettings(metadata, settings(), new IndexScopedSettings(Settings.EMPTY, settingSet));
  }

  protected Store createStore(String indexName, final Directory directory) throws IOException {
    return createStore(indexName, indexSettings(indexName), directory);
  }

  protected Store createStore(String indexName, final IndexSettings indexSettings,
                              final Directory directory) throws IOException {
    return new Store(shardId(indexName), indexSettings, directory, new DummyShardLock(shardId(indexName)));
  }

  @After
  public void clean() {
    deleteFiles(data);
    deleteFiles(translog);
  }

  @Before
  public void setup() {
    data = Paths.get("/Users/penghuo/tmp/opensearch/data/index");
    translog = Paths.get("/Users/penghuo/tmp/opensearch/data/translog");

    SearchModule
        searchModule = new SearchModule(settings(), Collections.EMPTY_LIST);
    NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
        Stream.of(
            NetworkModule.getNamedXContents().stream(),
            IndicesModule.getNamedXContents().stream(),
            searchModule.getNamedXContents().stream(),
            ClusterModule.getNamedXWriteables().stream()
        ).flatMap(Function.identity()).collect(toList())
    );

    fsRepository = new FsRepository(repositoryMetadata(), xContentRegistry, threadPool);
    fsRepository.start();
  }


  private FsRepository fsRepository;

  private Path data;
  private Path translog;

  @Test
  public void testSnapshotShard() throws IOException, InterruptedException {
    String indexName = "foo";
    String mapping = mappings();

    try (Store store = createStore(indexName, newFSDirectory(data));
         Engine engine = createEngine(indexSettings(indexName), store, translog, NoMergePolicy.INSTANCE)) {
      List<Segment> segments = engine.segments(true);
      assertThat(segments.isEmpty(), equalTo(true));

      ParsedDocument doc = document(indexName, "1", "{\n" +
          "  \"aInt\": 1,\n" +
          "  \"aString\": \"a\",\n" +
          "  \"aText\": \"i am first\"\n" +
          "}", mapping);
      engine.index(indexForDoc(doc));
      engine.refresh("test");

      segments = engine.segments(true);
      assertThat(segments.size(), equalTo(1));

      ParsedDocument doc2 = document(indexName, "2", "{\n" +
          "  \"aInt\": 2,\n" +
          "  \"aString\": \"b\",\n" +
          "  \"aText\": \"i am second\"\n" +
          "}", mapping);
      engine.index(indexForDoc(doc2));
      engine.refresh("test");

      segments = engine.segments(true);
      assertThat(segments.size(), equalTo(2));

      engine.flush();

      try (Engine.Searcher searcher = engine
          .acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
        final TotalHitCountCollector collector = new TotalHitCountCollector();
        searcher.search(new MatchAllDocsQuery(), collector);
        assertEquals(2, collector.getTotalHits());

        System.out.println("finish");
      }

      SnapshotId snapshotId = new SnapshotId("mySnapshot", UUID.randomUUID().toString());
      IndexId indexId = new IndexId(indexName, UUID.randomUUID().toString());
      IndexCommit indexCommit = engine.acquireLastIndexCommit(true).get();
      String shardStateIdentifier = getShardStateId(indexCommit);
      IndexShardSnapshotStatus indexShardSnapshotStatus =
          IndexShardSnapshotStatus.newInitializing(NEW_SHARD_GEN);
      Version version = V_3_0_0;
      Map<String, Object> userMetadata = new HashMap<>();
      final AtomicReference<String> indexGeneration = new AtomicReference<>();
      ActionListener<String> actionListener = new ActionListener<>() {
        @Override
        public void onResponse(String s) {
          indexGeneration.set(s);
        }

        @Override
        public void onFailure(Exception e) {
          fail(e.getMessage());
        }
      };

      CountDownLatch latch = new CountDownLatch(1);
      ActionListener<RepositoryData> repositoryDataActionListener =new ActionListener<>() {
        @Override
        public void onResponse(RepositoryData repo) {
          System.out.println("repo write success");
          latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
          latch.countDown();
          fail(e.getMessage());
        }
      };
      Metadata clusterMetaData = new Metadata
          .Builder()
          .version(V_3_0_0.id)
          .clusterUUID("_na_")
          .indices(Map.of(indexName, indexMetadata(indexName)))
          .build();

      fsRepository.snapshotShard(store, mapperService(indexName), snapshotId, indexId, indexCommit,
          shardStateIdentifier,
          indexShardSnapshotStatus, version, userMetadata, clusterMetaData, actionListener, repositoryDataActionListener);

      System.out.println("wait...");
      latch.await();

      System.out.println("success");
    }
  }


}
