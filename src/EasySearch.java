import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class EasySearch {

    private String queryString;
    private static EasySearch luceneUtils = new EasySearch();

    public static final String INDEX_DIR = "index/";
    public static final int CAPACITY = 1000;

    public EasySearch() {
    }

    public static void main(final String a[]) {
        File dirToIndex;
        Directory directory;
        MinMaxPriorityQueue<DocumentTopicsScore1> documentTopicsScores;

        try {
            luceneUtils.parseArguments(a);

            directory = FSDirectory.open(Paths.get(INDEX_DIR));

            documentTopicsScores = luceneUtils.queryIndex(directory, luceneUtils.getQueryString());

            System.out.println("Query: " + luceneUtils.getQueryString());
            System.out.println();

            System.out.println("Doc-ID        | Rank | Score");
            System.out.println("----------------------------------------------");
            for (int i = 1; i <= 1000 ; i++) {
                DocumentTopicsScore1 ds = documentTopicsScores.removeLast();
                System.out.println(ds.getDocName() + " | " + i + "    | " + ds.getScore());
            }

            directory.close();
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public void parseArguments(String[] arg) {
        luceneUtils.setQueryString(arg[0]);
    }


    public MinMaxPriorityQueue<DocumentTopicsScore1> queryIndex(Directory directory, String queryString) {
        IndexReader reader;
        IndexSearcher searcher;

        HashMap<String, Integer> docFreqMap;
        HashMap<String, Float> docLengthMap;
        HashMap<String, Double> docScoreMap;

        MinMaxPriorityQueue<DocumentTopicsScore1> documentTopicsScores = null;

        int numberOfDocs = luceneUtils.getDocumentCount(directory);

        try {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);

            Analyzer analyzer = new StandardAnalyzer();

            QueryParser parser = new QueryParser("TEXT", analyzer);
            Query query = parser.parse(queryString);

            // Query Terms
            Set<Term> queryTerms = new LinkedHashSet<Term>();
            searcher.createNormalizedWeight(query, false).extractTerms(queryTerms);

            docFreqMap = luceneUtils.listTermsDocFrequency(reader, queryTerms, query);

            DefaultSimilarity defaultSimilarity = new DefaultSimilarity();

            // Get segments of the index
            List<LeafReaderContext> leafContexts = reader.getContext().reader().leaves();

            docLengthMap = luceneUtils.generateDocLengthMap(defaultSimilarity, searcher, leafContexts);


//            PriorityQueue<DocumentTopicsScore1> documentTopicsScores = new PriorityQueue<DocumentTopicsScore1>(luceneUtils.CAPACITY, Collections.reverseOrder());
            documentTopicsScores = MinMaxPriorityQueue.create();

            System.out.println("Term      |  Doc Name     | Score" );
            for (int i = 0; i < leafContexts.size(); i++) {

                docScoreMap = new HashMap<String, Double>();

                // Get Document Length
                LeafReaderContext leafContext = leafContexts.get(i);
                int startDocNo = leafContext.docBase;
                int numberOfDoc = leafContext.reader().maxDoc();

                for (Term term : queryTerms) {
                    // Total number of doc having term ti
                    PostingsEnum de = MultiFields.getTermDocsEnum(leafContext.reader(), "TEXT", new BytesRef(term.text()));

                    int doc;
                    if (de != null) {
                        while ((doc = de.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {

                            int docNo = de.docID() + startDocNo;
                            String docName = searcher.doc(docNo).get("DOCNO");

                            float docLength = docLengthMap.get(docName);
                            int docFreq = docFreqMap.get(term.text());

                            double score = (de.freq() / docLength) * Math.log10(1 + ((double) numberOfDocs / docFreq));

                            if (docScoreMap.containsKey(docName)) {
                                score += docScoreMap.get(docName);
                            }

                            System.out.println(term.text() + " | " + docName + " | " + score );
                            docScoreMap.put(docName, score);
                        }
                    }
                }

                Iterator itr = docScoreMap.entrySet().iterator();

                while (itr.hasNext()) {
                    Map.Entry item = (Map.Entry) itr.next();

                    DocumentTopicsScore1 ds = new DocumentTopicsScore1((String) item.getKey(), (Double) item.getValue());

                    documentTopicsScores.offer(ds);

                    if (documentTopicsScores.size() > luceneUtils.CAPACITY) {
                        documentTopicsScores.removeFirst();
                    }

                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return documentTopicsScores;
    }

    /*
     * Get the query terms alongwith Document Frequency
     */
    public HashMap<String, Integer> listTermsDocFrequency(IndexReader reader, Set<Term> queryTerms, Query query) throws IOException {

        HashMap<String, Integer> docFreqMap = new HashMap<String, Integer>();

        for (Term term : queryTerms) {
            // Total number of doc having term ti
            int df = reader.docFreq(new Term("TEXT", term.text()));

            docFreqMap.put(term.text(), df);
        }

        return docFreqMap;
    }

    /*
     * Get Document Length generate Documents Map
     */
    public HashMap<String, Float> generateDocLengthMap(DefaultSimilarity defaultSimilarity, IndexSearcher searcher, List<LeafReaderContext> leafContexts) throws IOException {

        HashMap<String, Float> docLengthMap = new HashMap<String, Float>();

        // Processing each Segment
        for (int i = 0; i < leafContexts.size(); i++) {

            // Get Document Length
            LeafReaderContext leafContext = leafContexts.get(i);
            int startDocNo = leafContext.docBase;
            int numberOfDoc = leafContext.reader().maxDoc();

            for (int docId = 0; docId < numberOfDoc; docId++) {

                int docNo = docId + startDocNo;

                // Get normalized length (1/sqrt(numOfTokens)) of the document
                float normDocLength = defaultSimilarity.decodeNormValue(leafContext.reader().getNormValues("TEXT").get(docId));

                // Length of the document
                float docLength = 1 / (normDocLength * normDocLength);

                docLengthMap.put(searcher.doc(docNo).get("DOCNO"), docLength);

            }
        }
        return docLengthMap;
    }

    public int getDocumentCount(Directory directory) {
        int maxDoc = 0;

        try {
            DirectoryReader ireader = DirectoryReader.open(directory);
            maxDoc = ireader.maxDoc();

            ireader.close();
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return maxDoc;
    }

    public void deleteAllIndex(final Directory directory) throws IOException {
        final String existingDirectories[] = directory.listAll();
        for (final String existingDirectory : existingDirectories)
            directory.deleteFile(existingDirectory);
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }
}

class DocumentScore implements Comparable<DocumentTopicsScore1> {

    private String docName;
    private double score;


    DocumentScore(String docName, double score) {
        this.docName = docName;
        this.score = score;
    }

    @Override
    public int compareTo(DocumentTopicsScore1 ds) {
        return Double.compare(getScore(), ds.getScore());
    }

    public String getDocName() {
        return docName;
    }

    public void setDocName(String docName) {
        this.docName = docName;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }
}