package org.opensearch.index.snapshots;

import static java.util.stream.Collectors.toList;
import static org.opensearch.Version.V_3_0_0;
import static org.opensearch.index.snapshots.CommonsUtils.mapperService;
import static org.opensearch.repositories.ShardGenerations.NEW_SHARD_GEN;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.lucene.index.IndexCommit;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesModule;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.repositories.ShardGenerations;
import org.opensearch.respositories.FsRepository;
import org.opensearch.search.SearchModule;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.threadpool.ThreadPool;

public class MySnapshot {

  private final FsRepository fsRepository;

  private final SnapshotId snapshotId;

  private final IndexId indexId;

  private final Version version = V_3_0_0;

  public MySnapshot(Settings settings, String repositoryName, String snapshotName,
                    String indexName) {
    SearchModule
        searchModule = new SearchModule(settings, Collections.EMPTY_LIST);
    NamedXContentRegistry xContentRegistry = new NamedXContentRegistry(
        Stream.of(
            NetworkModule.getNamedXContents().stream(),
            IndicesModule.getNamedXContents().stream(),
            searchModule.getNamedXContents().stream(),
            ClusterModule.getNamedXWriteables().stream()
        ).flatMap(Function.identity()).collect(toList())
    );
    ThreadPool threadPool = new TestThreadPool(getClass().getName());

    this.snapshotId = new SnapshotId(snapshotName, UUID.randomUUID().toString());
    this.indexId = new IndexId(indexName, UUID.randomUUID().toString());
    this.fsRepository = new FsRepository(new RepositoryMetadata(repositoryName, "myTestType",
        settings), xContentRegistry, threadPool);
    fsRepository.start();
  }


  public void snapshotShard(String indexName, Store store, IndexCommit indexCommit) {
    try {

      String shardStateIdentifier = getShardStateId(indexCommit);
      IndexShardSnapshotStatus indexShardSnapshotStatus =
          IndexShardSnapshotStatus.newInitializing(NEW_SHARD_GEN);

      Map<String, Object> userMetadata = new HashMap<>();
      final AtomicReference<String> indexGeneration = new AtomicReference<>();
      CountDownLatch latch = new CountDownLatch(1);
      ActionListener<String> actionListener = new ActionListener<>() {
        @Override
        public void onResponse(String s) {
          indexGeneration.set(s);
          System.out.println("finish snapshot shard: " + store.shardId());
          latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
          throw new RuntimeException(e);
        }
      };
      fsRepository.snapshotShard(store, mapperService(indexName), snapshotId, indexId, indexCommit,
          shardStateIdentifier,
          indexShardSnapshotStatus, version, userMetadata, actionListener);
      latch.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public void finalizeSnapshot(Metadata clusterMetaData,
                               ActionListener<RepositoryData> repositoryDataActionListener) {
    // todo using fix shardId is ok?
    ShardGenerations shardGenerations = ShardGenerations.builder()
        .put(indexId, 0, UUID.randomUUID().toString())
        .build();
    long repositoryStateId = -1;
    SnapshotInfo snapshotInfo = new SnapshotInfo(
        snapshotId,
        Arrays.asList(indexId.getName()),
        Collections.EMPTY_LIST,
        System.currentTimeMillis(),
        null,
        System.currentTimeMillis() + 600,
        1,
        Collections.EMPTY_LIST,
        false,
        Collections.EMPTY_MAP,
        false
    );
    fsRepository.finalizeSnapshot(shardGenerations, repositoryStateId, clusterMetaData, snapshotInfo,
        Version.V_3_0_0, Function.identity(), repositoryDataActionListener);
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
}
