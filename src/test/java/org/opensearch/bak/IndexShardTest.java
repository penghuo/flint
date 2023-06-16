package org.opensearch.bak;

import static java.util.Collections.emptyMap;
import static org.opensearch.index.IndexService.IndexCreationContext.CREATE_INDEX;

import java.io.IOException;
import java.util.Collections;
import java.util.function.BiFunction;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.Version;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.breaker.CircuitBreaker;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.ShardLock;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.VersionType;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.engine.EngineConfigFactory;
import org.opensearch.index.engine.InternalEngineFactory;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.seqno.RetentionLeaseSyncer;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogFactory;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

public class IndexShardTest extends OpenSearchTestCase {

  private Index index;
  private Settings settings;
  private IndexSettings indexSettings;
  private Environment environment;
  private AnalysisRegistry emptyAnalysisRegistry;
  private NodeEnvironment nodeEnvironment;
  private IndicesQueryCache indicesQueryCache;

  private IndexService.ShardStoreDeleter deleter = new IndexService.ShardStoreDeleter() {
    @Override
    public void deleteShardStore(String reason, ShardLock lock, IndexSettings indexSettings)
        throws IOException {
    }

    @Override
    public void addPendingDelete(ShardId shardId, IndexSettings indexSettings) {
    }
  };

  private static class TestEnvironment {

    private TestEnvironment() {
    }

    public static Environment newEnvironment(Settings settings) {
      return new Environment(settings, null);
    }
  }

  private final IndexFieldDataCache.Listener listener = new IndexFieldDataCache.Listener() {
  };
  private MapperRegistry mapperRegistry;
  private ThreadPool threadPool;
  private CircuitBreakerService circuitBreakerService;
  private BigArrays bigArrays;
  private ScriptService scriptService;
  private ClusterService clusterService;
  private RepositoriesService repositoriesService;

  @Before
  public void setUp() throws Exception {
    settings = Settings.builder()
        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
        .put(Environment.PATH_HOME_SETTING.getKey(), "/Users/penghuo/tmp/opensearch")
        .put(Environment.PATH_DATA_SETTING.getKey(), "/Users/penghuo/tmp/opensearch/data")
        .build();
    indicesQueryCache = new IndicesQueryCache(settings);


//    IndexMetadata indexMetadata = IndexMetadata.builder("my-index")
//        .putMapping(new MappingMetadata("_doc", Map.of("name", Map.of("type", "text"))))
//        .build();
    indexSettings = IndexSettingsModule.newIndexSettings("foo", settings);

    index = indexSettings.getIndex();
    environment = TestEnvironment.newEnvironment(settings);
    emptyAnalysisRegistry = new AnalysisRegistry(
        environment,
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap(),
        emptyMap()
    );
    threadPool =
        new ThreadPool(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").put(settings).build());
    circuitBreakerService = new NoneCircuitBreakerService();
    PageCacheRecycler pageCacheRecycler = new PageCacheRecycler(settings);
    bigArrays = new BigArrays(pageCacheRecycler, circuitBreakerService, CircuitBreaker.REQUEST);
    scriptService = new ScriptService(settings, Collections.emptyMap(), Collections.emptyMap());
    clusterService = ClusterServiceUtils.createClusterService(threadPool);
    nodeEnvironment = new NodeEnvironment(settings, environment);
    mapperRegistry = new IndicesModule(Collections.emptyList()).getMapperRegistry();
  }

  private IndexModule newIndexModule() throws IOException {
    final IndexModule module = new IndexModule(
        indexSettings,
        emptyAnalysisRegistry,
        new InternalEngineFactory(),
        new EngineConfigFactory(indexSettings),
        Collections.emptyMap(),
        () -> true,
        new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY)),
        null
    );

    return module;
  }

  private IndexService newIndexService(IndexModule module) throws IOException {
    BiFunction<IndexSettings, ShardRouting, TranslogFactory>
        translogFactorySupplier = (indexSettings, shardRouting) -> {
      return new InternalTranslogFactory();
    };
    return module.newIndexService(
        CREATE_INDEX,
        nodeEnvironment,
        xContentRegistry(),
        deleter,
        circuitBreakerService,
        bigArrays,
        threadPool,
        scriptService,
        clusterService,
        null,
        indicesQueryCache,
        mapperRegistry,
        new IndicesFieldDataCache(settings, listener),
        writableRegistry(),
        () -> false,
        null,
        new RemoteSegmentStoreDirectoryFactory(() -> repositoriesService),
        translogFactorySupplier
    );
  }

  protected NamedXContentRegistry xContentRegistry() {
    return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
  }

  protected NamedWriteableRegistry writableRegistry() {
    return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
  }

  @Test
  public void writeToFS() throws IOException {
    IndexService indexService = newIndexService(newIndexModule());
    createShard(indexService);

    IndexShard primary = indexService.getShard(0);

    RecoveryState state = new RecoveryState(primary.routingEntry(),
        getFakeDiscoNode(primary.routingEntry().currentNodeId()), null);
    primary.markAsRecovering("recovery", state);

    recoverFromStore(primary);

    primary.applyIndexOperationOnPrimary(
        Versions.MATCH_ANY,
        VersionType.INTERNAL,
        new SourceToParse("test", "1", new BytesArray("{}"), XContentType.JSON),
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        0,
        IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
        false
    );
  }

  private IndexShard createShard(IndexService indexService) throws IOException {
    ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("test", "_na_", 0),
        true,
        RecoverySource.ExistingStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
    );
    ShardRouting newShard = shard.initialize("node1", null, -1);
    IndexShard indexShard = indexService.createShard(
        newShard,
        s -> {
        },
        RetentionLeaseSyncer.EMPTY,
        SegmentReplicationCheckpointPublisher.EMPTY,
        null
    );
    return indexShard;
  }

  /**
   * copy from storereovery.class internalRecoverFromStore
   * @param indexShard
   * @throws IOException
   */
  private void recoverFromStore(IndexShard indexShard) throws IOException {
    indexShard.prepareForIndexRecovery();
    final Store store = indexShard.store();
    store.incRef();

    store.createEmpty(indexShard.indexSettings().getIndexVersionCreated().luceneVersion);
    final String translogUUID = Translog.createEmptyTranslog(
        indexShard.shardPath().resolveTranslog(),
        SequenceNumbers.NO_OPS_PERFORMED,
        indexShard.shardId(),
        indexShard.getPendingPrimaryTerm()
    );
    store.associateIndexWithNewTranslog(translogUUID);
    indexShard.recoveryState().getIndex().setFileDetailsComplete();

    indexShard.openEngineAndRecoverFromTranslog();
//    indexShard.getEngine().fillSeqNoGaps(indexShard.getPendingPrimaryTerm());
    indexShard.finalizeRecovery();
    indexShard.postRecovery("post recovery from shard_store");
  }

  protected DiscoveryNode getFakeDiscoNode(String id) {
    return new DiscoveryNode(
        id,
        id,
        buildNewFakeTransportAddress(),
        Collections.emptyMap(),
        DiscoveryNodeRole.BUILT_IN_ROLES,
        Version.CURRENT
    );
  }
}
