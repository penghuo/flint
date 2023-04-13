package org.lucene;

import java.io.IOException;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Test;

public class InvertedIndexTest {
  @Test
  public void t1() throws IOException {
    Directory directory = new ByteBuffersDirectory();
    IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);

    FieldType type = new FieldType();
    type.setStored(true);
    type.setTokenized(true);
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

    Document doc = new Document();
    doc.add(new Field("content", "book book is", type));
    indexWriter.addDocument(doc);

    indexWriter.commit();
  }
}
