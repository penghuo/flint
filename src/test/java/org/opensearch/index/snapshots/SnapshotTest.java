package org.opensearch.index.snapshots;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.opensearch.Version.V_3_0_0;
import static org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hc.core5.http.HttpHost;
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
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.bak.IndexSettingsModule;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.UUIDs;
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
import org.opensearch.repositories.RepositoryData;
import org.opensearch.respositories.FsRepository;
import org.opensearch.search.SearchModule;
import org.opensearch.test.DummyShardLock;

public class SnapshotTest extends EngineTestCase {

  static int totalSize = 500;
  static int shardSize = 5;
  static int shardIdStart = 0;
  static String REPO_NAME = "flintsnapshot";

  private Settings settings() {
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, V_3_0_0)
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/daichen/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/daichen/tmp/opensearch/data")
        .put(FsRepository.REPOSITORIES_LOCATION_SETTING.getKey(), REPO_NAME)
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
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/daichen/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/daichen/tmp/opensearch/data")
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
        "  \"_doc\": {\n" +
        "    \"dynamic\": false,\n" +
        "    \"properties\": {\n" +
        "      \"timestamp\": {\n" +
        "        \"type\": \"date\",\n" +
        "        \"format\": \"strict_date_optional_time||epoch_millis\"\n" +
        "      },\n" +
        "      \"year\": {\n" +
        "        \"type\": \"integer\"\n" +
        "      },\n" +
        "      \"month\": {\n" +
        "        \"type\": \"integer\"\n" +
        "      },\n" +
        "      \"day\": {\n" +
        "        \"type\": \"integer\"\n" +
        "      },\n" +
        "      \"eventId\": {\n" +
        "        \"type\": \"keyword\"\n" +
        "      },\n" +
        "      \"statusCode\": {\n" +
        "        \"type\": \"integer\"\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}";
  }

  private ShardId shardId(String indexName, int shardId) {
    return new ShardId(new Index(indexName, "_na_"), shardId);
  }

  /**
   * {
   *   "_doc": {
   *     "dynamic": false,
   *     "properties": {
   *       "year": {
   *         "type": "integer"
   *       },
   *       "month": {
   *         "type": "integer"
   *       },
   *       "day": {
   *         "type": "integer"
   *       },
   *       "eventId": {
   *         "type": "keyword"
   *       },
   *       "statusCode": {
   *         "type": "integer"
   *       }
   *     }
   *   }
   * }
   */
  private IndexMetadata indexMetadata(String indexName) throws IOException {
    String mappings = "{\"_doc\":{\"dynamic\":false,\"properties\":{\"timestamp\":{\"type\":\"date\",\"format\":\"strict_date_optional_time||epoch_millis\"},\"year\":{\"type\":\"integer\"},\"month\":{\"type\":\"integer\"},\"day\":{\"type\":\"integer\"},\"eventId\":{\"type\":\"keyword\"},\"statusCode\":{\"type\":\"integer\"}}}}";
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, V_3_0_0)
        .build();
    return IndexMetadata.builder(indexName)
        .version(V_3_0_0.id)
        .settings(settings)
        .numberOfReplicas(0)
        .numberOfShards(shardSize)
        .putMapping(mappings)
        .state(IndexMetadata.State.OPEN)
        .build();
  }

  private IndexSettings indexSettings(String indexName) throws IOException {
    IndexMetadata metadata = indexMetadata(indexName);
    Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
    return new IndexSettings(metadata, settings(),
        new IndexScopedSettings(Settings.EMPTY, settingSet));
  }

  protected Store createStore(String indexName, final Directory directory, int shardId)
      throws IOException {
    return createStore(indexName, indexSettings(indexName), directory, shardId);
  }

  protected Store createStore(String indexName, final IndexSettings indexSettings,
                              final Directory directory, int shardId) throws IOException {
    return new Store(shardId(indexName, shardId), indexSettings, directory,
        new DummyShardLock(shardId(indexName, shardId)));
  }

  @After
  public void clean() {
//    deleteFiles(data);
//    deleteFiles(translog);
  }

  @Before
  public void setup() {
    data = Paths.get("/Users/daichen/tmp/opensearch/data/index");
    translog = Paths.get("/Users/daichen/tmp/opensearch/data/translog");

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
  private static Random random = new Random();

  private static class InputData {
    private final long timestamp;
    private final int year;
    private final int month;
    private final int day;
    private final String eventId;
    private final int statusCode;

    public static InputData data(int dayOfMonth) {
      LocalDateTime startDateTime = LocalDateTime.of(2023, 6, dayOfMonth, 0, 0, 0);
      LocalDateTime endDateTime = LocalDateTime.of(2023, 6, dayOfMonth, 23, 59, 59);

      long startTimestamp = startDateTime.toEpochSecond(ZoneOffset.UTC) * 1000;
      long endTimestamp = endDateTime.toEpochSecond(ZoneOffset.UTC) * 1000;

      return new InputData(generateTimestamp(startTimestamp, endTimestamp), startDateTime.getYear(),
          startDateTime.getMonthValue(),
          startDateTime.getDayOfMonth(), UUID.randomUUID().toString(), generateRandomValue(1,
          batchSize * 10));
    }

    public static int generateRandomValue(int numerator, int denominator) {
      int randomNumber = random.nextInt(denominator);

      if (randomNumber < numerator) {
        return 404;
      } else {
        return 200;
      }
    }

    private static long generateTimestamp(long startTimestamp, long endTimestamp) {
      long randomTimestamp = startTimestamp + (long) (Math.random() * (endTimestamp - startTimestamp));
      return randomTimestamp;
    }

    public static LocalDate generateRandomDate(LocalDate startDate, LocalDate endDate) {
      Random random = new Random();
      long startEpochDay = startDate.toEpochDay();
      long endEpochDay = endDate.toEpochDay();
      long randomDay = startEpochDay + random.nextInt((int) (endEpochDay - startEpochDay));

      return LocalDate.ofEpochDay(randomDay);
    }

    public InputData(long timestamp, int year, int month, int day, String eventId, int statusCode) {
      this.timestamp = timestamp;
      this.year = year;
      this.month = month;
      this.day = day;
      this.eventId = eventId;
      this.statusCode = statusCode;
    }

    public String toJson() {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    public int getYear() {
      return year;
    }

    public int getMonth() {
      return month;
    }

    public int getDay() {
      return day;
    }

    public String getEventId() {
      return eventId;
    }

    public int getStatusCode() {
      return statusCode;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }

  public List<InputData> mockInputData(int size) {
    return mockInputData(size, 1);
  }


  /**
   * create mock data.
   *
   * @param size
   * @return
   */
  public List<InputData> mockInputData(int size, int dayOfMonth) {
    List<InputData> inputDataList = new ArrayList<>();

    for (int i = 0; i < size; i++) {
      inputDataList.add(InputData.data(dayOfMonth));
    }
    return inputDataList;
  }

  /**
   * shard id -> shard data.
   */
  public static Map<Integer, List<InputData>> splitList(List<InputData> originalList,
                                                        int batchSize) {
    Map<Integer, List<InputData>> splitLists = new HashMap<>();
    int shardId = shardIdStart;
    for (int i = 0; i < originalList.size(); i += batchSize) {
      int endIndex = Math.min(i + batchSize, originalList.size());
      List<InputData> splitList = originalList.subList(i, endIndex);
      splitLists.put(shardId, splitList);
      shardId++;
    }

    return splitLists;
  }

  public static void describeData(Map<Integer, List<InputData>> data) {
    Set<Integer> shardIds = new HashSet<>();
    for (Map.Entry<Integer, List<InputData>> entry : data.entrySet()) {
      if (entry.getValue().stream().anyMatch(d -> d.statusCode == 404)) {
        shardIds.add(entry.getKey());
      }
    }
    System.out.println("========== describe data ==========");
    System.out.println("shard id include 404");

    for (Integer shardId : shardIds) {
      System.out.println(shardId);
    }
  }

  public static void describeData(int shardId, List<InputData> data,
                                  Map<Integer, Set<String>> shardIdEventIdMapping) {
    Set<String> eventIDs = new HashSet<>();
    long errorCount = data.stream().map(v -> {
      if (v.statusCode == 404) {
        eventIDs.add(v.eventId);
      }
      return v;
    }).filter(v -> v.statusCode == 404).count();

    if (errorCount > 0) {
      shardIdEventIdMapping.put(shardId, eventIDs);
    }

    System.out.println("\n\n============== describe data ==============\n\n");

    System.out.println("Total 404 rows: " +
        shardIdEventIdMapping.values().stream().map(Set::size).mapToInt(Integer::intValue).sum() + "\n");
    for (int i = 0; i <= shardId; i++) {
      if (shardIdEventIdMapping.containsKey(i)) {
        System.out.println("shardId: " + i);
        for (String eventID : shardIdEventIdMapping.get(i)) {
          System.out.println("--> " + eventID);
        }
      }
    }
    System.out.println("\n\n===========================================\n\n");
  }

  @Test
  public void testHackathon() throws IOException, InterruptedException {
    String indexName = "foo";
    String mapping = mappings();

    Map<Integer, List<InputData>> shardInputDataMap = splitList(mockInputData(totalSize), batchSize);

    describeData(shardInputDataMap);

    System.out.println("========== start snapshot ==========");
    MySnapshot snapshot = new MySnapshot(settings(), "myRepository", "mySnapshot", indexName);
    for (Map.Entry<Integer, List<InputData>> entry : shardInputDataMap.entrySet()) {
      int shardId = entry.getKey();
      List<InputData> dataList = entry.getValue();

      Path base = base(shardId);
      Path dataPath = data(base);
      Path translogPath = translog(base);

      Store store = createStore(indexName, newFSDirectory(dataPath), shardId);
      Engine engine = createEngine(indexSettings(indexName), store, translogPath,
          NoMergePolicy.INSTANCE);
      indexShard(engine, indexName, mapping, dataList, shardId);
      snapshot.snapshotShard(indexName, store, engine.acquireLastIndexCommit(true).get());
    }
    Metadata clusterMetaData = new Metadata
        .Builder()
        .version(V_3_0_0.id)
        .clusterUUID("_na_")
        .indices(Map.of(indexName, indexMetadata(indexName)))
        .build();
    CountDownLatch latch = new CountDownLatch(1);
    ActionListener<RepositoryData> listener = new ActionListener<>() {
      @Override
      public void onResponse(RepositoryData repo) {
        System.out.println("finalize Snapshot success");
        latch.countDown();
      }

      @Override
      public void onFailure(Exception e) {
        latch.countDown();
        fail(e.getMessage());
      }
    };
    snapshot.finalizeSnapshot(clusterMetaData, listener);

    System.out.println("wait...");
    latch.await();
    System.out.println("success");
  }

  private void indexShard(Engine engine, String indexName, String mapping,
                          List<InputData> dataList, int shardId) throws IOException {
    List<Segment> segments = engine.segments(true);

    for (InputData inputData : dataList) {
      ParsedDocument doc = document(indexName, UUIDs.base64UUID(), inputData.toJson(), mapping);
      engine.index(indexForDoc(doc));
    }
    engine.forceMerge(true, 1, false, true, false, UUIDs.base64UUID());
    engine.flush();

    segments = engine.segments(true);
//    System.out.println("shardId: " + shardId);
//    assertThat(segments.size(), equalTo(1));

    try (Engine.Searcher searcher = engine
        .acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
      final TotalHitCountCollector collector = new TotalHitCountCollector();
      searcher.search(new MatchAllDocsQuery(), collector);
      assertEquals(dataList.size(), collector.getTotalHits());
    }
  }

  private final String basePath = "/Users/daichen/tmp/opensearch/alb_logs/";
  private Path base(int shardId) throws IOException {
    Path path = Paths.get(basePath + shardId);
    return Files.createDirectories(path);
  }

  private Path data(Path basePath) throws IOException {
    Path path = basePath.resolve("data");
    return Files.createDirectories(path);
  }
  private Path translog(Path basePath) throws IOException {
    Path path = basePath.resolve("translog");
    return Files.createDirectories(path);
  }































  /**
   * Hackathon 2023 Demo.
   */
  static int batchSize = 1000;
  static int sleepInterval = 10;
  static int iteration = 30;

  @Test
  public void testHackathonSeq() throws IOException, InterruptedException {
    String indexName = "alb_logs";
    String mapping = mappings();

    MySnapshot snapshot = new MySnapshot(settings(), "myRepository", "mySnapshot", indexName);

    List<InputData> inputData = mockInputData(batchSize, 1);
    Map<Integer, Set<String>> shardIdEventIdMapping = new HashMap<>();

    describeData(0, inputData, shardIdEventIdMapping);
    snapshotShard(indexName, mapping, 0, inputData, snapshot);

    createSkippingIndex();
    refreshSkippingIndex(0, inputData);

    Metadata clusterMetaData = new Metadata
        .Builder()
        .version(V_3_0_0.id)
        .clusterUUID("_na_")
        .indices(Map.of(indexName, indexMetadata(indexName)))
        .build();
    CountDownLatch latch = new CountDownLatch(1);
    ActionListener<RepositoryData> listener = new ActionListener<>() {
      @Override
      public void onResponse(RepositoryData repo) {
        System.out.println("finalizeSnapshot success");
        latch.countDown();
      }

      @Override
      public void onFailure(Exception e) {
        latch.countDown();
        fail(e.getMessage());
      }
    };
    snapshot.finalizeSnapshot(clusterMetaData, listener);

    latch.await();
    System.out.println("initialized");


    for (int i = 1; i < iteration; i++) {
      TimeUnit.SECONDS.sleep(sleepInterval);

      inputData = mockInputData(batchSize, i + 1);
      snapshotShard(indexName, mapping, i, inputData, snapshot);

      describeData(i, inputData, shardIdEventIdMapping);
      refreshSkippingIndex(i, inputData);
    }
  }

  protected void snapshotShard(String indexName, String mapping, int shardId,
                               List<InputData> dataList, MySnapshot snapshot) throws IOException {
    Path base = base(shardId);
    Path dataPath = data(base);
    Path translogPath = translog(base);

    Store store = createStore(indexName, newFSDirectory(dataPath), shardId);
    Engine engine = createEngine(indexSettings(indexName), store, translogPath,
        NoMergePolicy.INSTANCE);
    indexShard(engine, indexName, mapping, dataList, shardId);
    snapshot.snapshotShard(indexName, store, engine.acquireLastIndexCommit(true).get());
  }

  public static final String SKIPPING_INDEX_NAME = "flint_virtual_alb_logs_skipping_index";

  private void createSkippingIndex() {
    System.out.println("Start creating skipping index: " + SKIPPING_INDEX_NAME);

    try (RestHighLevelClient client = createOpenSearchClient()) {
      CreateIndexRequest createIndexRequest = new CreateIndexRequest(SKIPPING_INDEX_NAME);
      String mapping = "{"
          + "\"mappings\": {"
          + "  \"_meta\": {"
          + "    \"indexedColumns\": [\"statusCode\"]"
          + "  },"
          + "  \"properties\": {"
          + "    \"statusCode\": {\"type\": \"integer\"},"
          + "    \"shardId\": {\"type\": \"integer\"}"
          + "  }"
          + "}"
          + "}";
      createIndexRequest.source(mapping, XContentType.JSON);

      CreateIndexResponse response = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);

      if (response.isAcknowledged()) {
        System.out.println("Create skipping index successfully");
      } else {
        System.out.println("Failed to create skipping index");
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // Convert shardIdEventIdMapping to OpenSearch bulk request with {key: values.size()} and send it to an OpenSearch cluster
  private void refreshSkippingIndex(int shardId, List<InputData> batch) {
    System.out.println("Start refreshing skipping index: shardId " + shardId);

    try (RestHighLevelClient client = createOpenSearchClient()) {
      Set<Integer> statusCodes = batch.stream()
          .map(InputData::getStatusCode)
          .collect(Collectors.toSet());
      System.out.println("Status code value set: " + statusCodes);

      // Ingest the document
      Map<String, Object> document = new HashMap<>();
      document.put("shardId", shardId);
      document.put("statusCode", new ArrayList<>(statusCodes));

      IndexRequest request = new IndexRequest(SKIPPING_INDEX_NAME).source(document, XContentType.JSON);
      IndexResponse response = client.index(request, RequestOptions.DEFAULT);

      // Handle the response as needed
      if (response.getResult() == DocWriteResponse.Result.CREATED || response.getResult() == DocWriteResponse.Result.UPDATED) {
        System.out.println("Refresh skipping index successfully");
      } else {
        // Failed to index document
        System.out.println("Failed to refresh skipping index: " + response.getResult());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private RestHighLevelClient createOpenSearchClient() {
    return new RestHighLevelClient(
        RestClient.builder(new HttpHost("http", "localhost", 9200)));
  }

}
