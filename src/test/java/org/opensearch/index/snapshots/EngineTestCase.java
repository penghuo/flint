/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.snapshots;

import static java.util.Collections.emptyList;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.ToLongBiFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.Index;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.TranslogHandler;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.seqno.LocalCheckpointTracker;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.threadpool.ThreadPool;

public abstract class EngineTestCase {

  protected final ShardId shardId = new ShardId(new Index("alb_logs", "_na_"), 0);
  protected final AllocationId allocationId = AllocationId.newInitializing();
  protected final PrimaryTermSupplier
      primaryTerm = new PrimaryTermSupplier(1L);
  protected ThreadPool threadPool = new TestThreadPool(getClass().getName());
  protected final Logger logger = LogManager.getLogger(getClass());

  public static final class PrimaryTermSupplier implements LongSupplier {
    private final AtomicLong term;

    PrimaryTermSupplier(long initialTerm) {
      this.term = new AtomicLong(initialTerm);
    }

    public long get() {
      return term.get();
    }

    public void set(long newTerm) {
      this.term.set(newTerm);
    }

    @Override
    public long getAsLong() {
      return get();
    }
  }

  public static long randomNonNegativeLong() {
    long randomLong = new Random().nextLong();
    return randomLong == Long.MIN_VALUE ? 0 : Math.abs(randomLong);
  }

  protected TranslogHandler createTranslogHandler(IndexSettings indexSettings, Engine engine) {
    return new TranslogHandler(xContentRegistry(), indexSettings, engine);
  }

  protected InternalEngine createEngine(IndexSettings indexSettings, Store store, Path translogPath,
                                        MergePolicy mergePolicy)
      throws IOException {
    return createEngine(indexSettings, store, translogPath, mergePolicy, null);

  }

  protected InternalEngine createEngine(
      IndexSettings indexSettings,
      Store store,
      Path translogPath,
      MergePolicy mergePolicy,
      @Nullable org.opensearch.index.engine.EngineTestCase.IndexWriterFactory indexWriterFactory
  ) throws IOException {
    return createEngine(indexSettings, store, translogPath, mergePolicy, indexWriterFactory, null,
        null);
  }

  protected InternalEngine createEngine(
      IndexSettings indexSettings,
      Store store,
      Path translogPath,
      MergePolicy mergePolicy,
      @Nullable org.opensearch.index.engine.EngineTestCase.IndexWriterFactory indexWriterFactory,
      @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
      @Nullable LongSupplier globalCheckpointSupplier
  ) throws IOException {
    return createEngine(
        indexSettings,
        store,
        translogPath,
        mergePolicy,
        indexWriterFactory,
        localCheckpointTrackerSupplier,
        null,
        null,
        globalCheckpointSupplier
    );
  }

  protected InternalEngine createEngine(
      IndexSettings indexSettings,
      Store store,
      Path translogPath,
      MergePolicy mergePolicy,
      @Nullable org.opensearch.index.engine.EngineTestCase.IndexWriterFactory indexWriterFactory,
      @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
      @Nullable LongSupplier globalCheckpointSupplier,
      @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation
  ) throws IOException {
    return createEngine(
        indexSettings,
        store,
        translogPath,
        mergePolicy,
        indexWriterFactory,
        localCheckpointTrackerSupplier,
        seqNoForOperation,
        null,
        globalCheckpointSupplier
    );
  }

  protected InternalEngine createEngine(
      IndexSettings indexSettings,
      Store store,
      Path translogPath,
      MergePolicy mergePolicy,
      @Nullable org.opensearch.index.engine.EngineTestCase.IndexWriterFactory indexWriterFactory,
      @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
      @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
      @Nullable Sort indexSort,
      @Nullable LongSupplier globalCheckpointSupplier
  ) throws IOException {
    EngineConfig
        config = config(indexSettings, store, translogPath, mergePolicy, null, indexSort,
        globalCheckpointSupplier);
    return createEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation,
        config);
  }

  protected InternalEngine createEngine(EngineConfig config) throws IOException {
    return createEngine(null, null, null, config);
  }

  protected InternalEngine createEngine(
      @Nullable org.opensearch.index.engine.EngineTestCase.IndexWriterFactory indexWriterFactory,
      @Nullable BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
      @Nullable ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
      EngineConfig config
  ) throws IOException {
    final Store store = config.getStore();
    final Directory directory = store.directory();
    if (Lucene.indexExists(directory) == false) {
      store.createEmpty(config.getIndexSettings().getIndexVersionCreated().luceneVersion);
      final String translogUuid = Translog.createEmptyTranslog(
          config.getTranslogConfig().getTranslogPath(),
          SequenceNumbers.NO_OPS_PERFORMED,
          shardId,
          primaryTerm.get()
      );
      store.associateIndexWithNewTranslog(translogUuid);

    }
    InternalEngine internalEngine =
        createInternalEngine(indexWriterFactory, localCheckpointTrackerSupplier, seqNoForOperation,
            config);
    TranslogHandler translogHandler = createTranslogHandler(config.getIndexSettings(),
        internalEngine);
    internalEngine.translogManager()
        .recoverFromTranslog(translogHandler, internalEngine.getProcessedLocalCheckpoint(),
            Long.MAX_VALUE);
    return internalEngine;
  }

  public static InternalEngine createInternalEngine(
      @Nullable
      final org.opensearch.index.engine.EngineTestCase.IndexWriterFactory indexWriterFactory,
      @Nullable final BiFunction<Long, Long, LocalCheckpointTracker> localCheckpointTrackerSupplier,
      @Nullable final ToLongBiFunction<Engine, Engine.Operation> seqNoForOperation,
      final EngineConfig config
  ) {
    return new InternalEngine(config);

  }

  private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY =
      new NamedXContentRegistry(
          ClusterModule.getNamedXWriteables()
      );

  protected static final NamedWriteableRegistry DEFAULT_NAMED_WRITABLE_REGISTRY =
      new NamedWriteableRegistry(
          ClusterModule.getNamedWriteables()
      );

  /**
   * The {@link NamedXContentRegistry} to use for this test. Subclasses should override and use liberally.
   */
  protected NamedXContentRegistry xContentRegistry() {
    return DEFAULT_NAMED_X_CONTENT_REGISTRY;
  }

  public EngineConfig config(
      IndexSettings indexSettings,
      Store store,
      Path translogPath,
      MergePolicy mergePolicy,
      ReferenceManager.RefreshListener refreshListener
  ) {
    return config(indexSettings, store, translogPath, mergePolicy, refreshListener, null,
        () -> SequenceNumbers.NO_OPS_PERFORMED);
  }

  public EngineConfig config(
      IndexSettings indexSettings,
      Store store,
      Path translogPath,
      MergePolicy mergePolicy,
      ReferenceManager.RefreshListener refreshListener,
      Sort indexSort,
      LongSupplier globalCheckpointSupplier
  ) {
    return config(
        indexSettings,
        store,
        translogPath,
        mergePolicy,
        refreshListener,
        indexSort,
        globalCheckpointSupplier,
        globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY
    );
  }

  public EngineConfig config(
      final IndexSettings indexSettings,
      final Store store,
      final Path translogPath,
      final MergePolicy mergePolicy,
      final ReferenceManager.RefreshListener refreshListener,
      final Sort indexSort,
      final LongSupplier globalCheckpointSupplier,
      final Supplier<RetentionLeases> retentionLeasesSupplier
  ) {
    return config(
        indexSettings,
        store,
        translogPath,
        mergePolicy,
        refreshListener,
        null,
        indexSort,
        globalCheckpointSupplier,
        retentionLeasesSupplier,
        new NoneCircuitBreakerService()
    );
  }

  public EngineConfig config(
      final IndexSettings indexSettings,
      final Store store,
      final Path translogPath,
      final MergePolicy mergePolicy,
      final ReferenceManager.RefreshListener externalRefreshListener,
      final ReferenceManager.RefreshListener internalRefreshListener,
      final Sort indexSort,
      final @Nullable LongSupplier maybeGlobalCheckpointSupplier,
      final @Nullable Supplier<RetentionLeases> maybeRetentionLeasesSupplier,
      final CircuitBreakerService breakerService
  ) {
    final IndexWriterConfig iwc = new IndexWriterConfig();
    final TranslogConfig
        translogConfig =
        new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
    final Engine.EventListener eventListener = new Engine.EventListener() {
    }; // we don't need to notify anybody in this test
    final List<ReferenceManager.RefreshListener> extRefreshListenerList =
        externalRefreshListener == null
            ? emptyList()
            : Collections.singletonList(externalRefreshListener);
    final List<ReferenceManager.RefreshListener> intRefreshListenerList =
        internalRefreshListener == null
            ? emptyList()
            : Collections.singletonList(internalRefreshListener);
    final LongSupplier globalCheckpointSupplier;
    final Supplier<RetentionLeases> retentionLeasesSupplier;
    if (maybeGlobalCheckpointSupplier == null) {
      assert maybeRetentionLeasesSupplier == null;
      final ReplicationTracker replicationTracker = new ReplicationTracker(
          shardId,
          allocationId.getId(),
          indexSettings,
          randomNonNegativeLong(),
          SequenceNumbers.NO_OPS_PERFORMED,
          update -> {
          },
          () -> 0L,
          (leases, listener) -> listener.onResponse(new ReplicationResponse()),
          () -> SafeCommitInfo.EMPTY
      );
      globalCheckpointSupplier = replicationTracker;
      retentionLeasesSupplier = replicationTracker::getRetentionLeases;
    } else {
      assert maybeRetentionLeasesSupplier != null;
      globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
      retentionLeasesSupplier = maybeRetentionLeasesSupplier;
    }
    return new EngineConfig.Builder().shardId(shardId)
        .threadPool(threadPool)
        .indexSettings(indexSettings)
        .warmer(null)
        .store(store)
        .mergePolicy(mergePolicy)
        .analyzer(iwc.getAnalyzer())
        .similarity(iwc.getSimilarity())
        .codecService(new CodecService(null, logger))
        .eventListener(eventListener)
        .queryCache(IndexSearcher.getDefaultQueryCache())
        .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
        .translogConfig(translogConfig)
        .flushMergesAfter(TimeValue.timeValueMinutes(5))
        .externalRefreshListener(extRefreshListenerList)
        .internalRefreshListener(intRefreshListenerList)
        .indexSort(indexSort)
        .circuitBreakerService(breakerService)
        .globalCheckpointSupplier(globalCheckpointSupplier)
        .retentionLeasesSupplier(retentionLeasesSupplier)
        .primaryTermSupplier(primaryTerm)
        .tombstoneDocSupplier(tombstoneDocSupplier())
        .build();
  }

  protected EngineConfig config(
      EngineConfig config,
      Store store,
      Path translogPath,
      EngineConfig.TombstoneDocSupplier tombstoneDocSupplier
  ) {
    IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
        "test",
        Settings.builder()
            .put(config.getIndexSettings().getSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .build()
    );
    TranslogConfig translogConfig =
        new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
    return new EngineConfig.Builder().shardId(config.getShardId())
        .threadPool(config.getThreadPool())
        .indexSettings(indexSettings)
        .warmer(config.getWarmer())
        .store(store)
        .mergePolicy(config.getMergePolicy())
        .analyzer(config.getAnalyzer())
        .similarity(config.getSimilarity())
        .codecService(new CodecService(null, logger))
        .eventListener(config.getEventListener())
        .queryCache(config.getQueryCache())
        .queryCachingPolicy(config.getQueryCachingPolicy())
        .translogConfig(translogConfig)
        .flushMergesAfter(config.getFlushMergesAfter())
        .externalRefreshListener(config.getExternalRefreshListener())
        .internalRefreshListener(config.getInternalRefreshListener())
        .indexSort(config.getIndexSort())
        .circuitBreakerService(config.getCircuitBreakerService())
        .globalCheckpointSupplier(config.getGlobalCheckpointSupplier())
        .retentionLeasesSupplier(config.retentionLeasesSupplier())
        .primaryTermSupplier(config.getPrimaryTermSupplier())
        .tombstoneDocSupplier(tombstoneDocSupplier)
        .build();
  }

  public static EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
    return new EngineConfig.TombstoneDocSupplier() {
      @Override
      public ParsedDocument newDeleteTombstoneDoc(String id) {
        final ParseContext.Document doc = new ParseContext.Document();
        Field uidField = new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        doc.add(uidField);
        Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
        doc.add(versionField);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        seqID.tombstoneField.setLongValue(1);
        doc.add(seqID.tombstoneField);
        return new ParsedDocument(
            versionField,
            seqID,
            id,
            null,
            Collections.singletonList(doc),
            new BytesArray("{}"),
            XContentType.JSON,
            null
        );
      }

      @Override
      public ParsedDocument newNoopTombstoneDoc(String reason) {
        final ParseContext.Document doc = new ParseContext.Document();
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        seqID.tombstoneField.setLongValue(1);
        doc.add(seqID.tombstoneField);
        Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
        doc.add(versionField);
        BytesRef byteRef = new BytesRef(reason);
        doc.add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return new ParsedDocument(versionField, seqID, null, null, Collections.singletonList(doc), null, XContentType.JSON, null);
      }
    };
  }

  protected Engine.Index indexForDoc(ParsedDocument doc) {
    return new Engine.Index(newUid(doc), primaryTerm.get(), doc);
  }

  public static Term newUid(String id) {
    return new Term("_id", Uid.encodeId(id));
  }

  public static Term newUid(ParsedDocument doc) {
    return newUid(doc.id());
  }

  protected Directory newFSDirectory(Path location) throws IOException {
    return new NIOFSDirectory(location, NativeFSLockFactory.INSTANCE);
  }
}
