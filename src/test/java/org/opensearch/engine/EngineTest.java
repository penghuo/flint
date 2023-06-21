package org.opensearch.engine;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TotalHitCountCollector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.Version;
import org.opensearch.bak.IndexSettingsModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesModule;
import org.opensearch.search.internal.ContextIndexSearcher;

public class EngineTest extends EngineTestCase {

  private Path data;
  private Path translog;

  @Before
  public void setup() {
    data = Paths.get("/Users/penghuo/tmp/opensearch/data/index");
    translog = Paths.get("/Users/penghuo/tmp/opensearch/data/translog");
  }

  @After
  public void clean() {
    deleteFiles(data);
    deleteFiles(translog);
  }

  public void testVerboseSegments() throws Exception {
    String indexName = "foo";
    String mapping = "{\n" +
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

    try (Store store = createStore(newFSDirectory(data));
         Engine engine = createEngine(defaultSettings, store, translog, NoMergePolicy.INSTANCE)) {
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

      try (Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL)) {
        final TotalHitCountCollector collector = new TotalHitCountCollector();
        searcher.search(new MatchAllDocsQuery(), collector);
        assertEquals(2, collector.getTotalHits());

        System.out.println("finish");
      }
    }
  }

  public void testRead() throws Exception {
    String indexName = "foo";
    String mapping = "{\n" +
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

    try (Store store = createStore(newFSDirectory(data));
         Engine engine = createEngine(defaultSettings, store, translog, NoMergePolicy.INSTANCE)) {
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

      // read
      Engine.Searcher engineSearcher = engine.acquireSearcher("search");
      ContextIndexSearcher searcher = new ContextIndexSearcher(
          engineSearcher.getIndexReader(),
          engineSearcher.getSimilarity(),
          engineSearcher.getQueryCache(),
          engineSearcher.getQueryCachingPolicy(),
          false,
          Executors.newFixedThreadPool(10),
          null
      );

//      FlintSearchContext
//          searchContext = new FlintSearchContext(queryShardContext(indexName), null, searcher);
//      FlintFetchPhase fetchPhase = new FlintFetchPhase(Arrays.asList(new FetchSourcePhase()));
//      fetchPhase.execute(searchContext);
    }
  }


  public void testParseDocument() throws Exception {
    ParsedDocument doc = document("foo", "1", "{\n" +
        "  \"aInt\": 1,\n" +
        "  \"aString\": \"a\",\n" +
        "  \"aText\": \"i am first\"\n" +
        "}",
        "{\n" +
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
            "}");

    assertTrue(doc.dynamicMappingsUpdate() == null);
  }

  private ParsedDocument document(String index, String id, String json, String mapping)
      throws IOException {
    MapperService mapperService = mapperService(index);
    DocumentMapper docMapper = mapperService.parse(SINGLE_MAPPING_NAME, new CompressedXContent(mapping));

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


  private Settings settings() {
    Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/penghuo/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/penghuo/tmp/opensearch/data")
        .build();
    return settings;
  }
}
