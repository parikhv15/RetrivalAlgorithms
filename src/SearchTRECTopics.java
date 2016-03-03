import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class SearchTRECTopics {

    private String queryString;
    private static SearchTRECTopics luceneUtils = new SearchTRECTopics();
    private static TrecTextDocumentUtils utils = new TrecTextDocumentUtils();

    public static final String INDEX_DIR = "index/";
    public static final String TOPICS_FILE = "topics.51-100";
    public static final String OUTPUTFILESHORT = "OutputShortQuery.txt";
    public static final String OUTPUTFILELONG = "OutputLongQuery.txt";
    public static final int CAPACITY = 1000;

    public SearchTRECTopics() {
    }

    public static void main(final String a[]) {
        FileWriter fw;
        File dirToIndex;
        Directory directory;
        MinMaxPriorityQueue<DocumentTopicsScore> documentTopicsScores;

        long startTime = System.currentTimeMillis();

        try {

            File fileToQuery = new File(TOPICS_FILE);

            String fileString = FileUtils.readFileToString(fileToQuery);
            String[] docElements = utils.getElements(fileString, "top", "/top");

            directory = FSDirectory.open(Paths.get(INDEX_DIR));

            fw = new FileWriter(OUTPUTFILESHORT);

            for (String docElement : docElements) {
                TrecTextDocument queryDoc = utils.parseDocument(docElement);

                documentTopicsScores = luceneUtils.queryIndex(directory, queryDoc.getTitle());
//                System.out.println("Query No | Doc-ID        | Rank | Type      | Score");
//                System.out.println("----------------------------------------------");
//                fw.write("Query No | Doc-ID        | Rank | Type      | Score"+"\n");
//                fw.write("--------------------------------------------------------------"+"\n");

                for (int i = 1; i <= 1000; i++) {
                    if (!documentTopicsScores.isEmpty()) {
                        DocumentTopicsScore ds = documentTopicsScores.removeLast();
//                        System.out.println(queryDoc.getDocNo() + "     |" + ds.getDocName() + " | " + i + "    | BM25_short | " + ds.getScore());
//                        fw.write(queryDoc.getDocNo() + "     |" + ds.getDocName() + " | " + i + "    | BM25_short | " + ds.getScore()+"\n");
//                        ds.getScore().setPrecision(5);
                        fw.write(queryDoc.getDocNo() + " " + "0" + " " + ds.getDocName() + " " + i + " " + new DecimalFormat("#0.00000").format(ds.getScore()) + " " + "DEFAULT_SHORT" + "\n");

                    }
                }
            }

            fw.close();
            fw = new FileWriter(OUTPUTFILELONG);
//
            for (String docElement : docElements) {
                TrecTextDocument queryDoc = utils.parseDocument(docElement);

                documentTopicsScores = luceneUtils.queryIndex(directory, queryDoc.getDesc());
//                fw.write("Query No | Doc-ID        | Rank | Type      | Score" + "\n");
//                fw.write("------------------------------------------------------" + "\n");
                for (int i = 1; i <= 1000; i++) {
                    if (!documentTopicsScores.isEmpty()) {
                        DocumentTopicsScore ds = documentTopicsScores.removeLast();
//                                                              fw.write(queryDoc.getDocNo() + " " + ds.getDocName() + " " + i + " " + ds.getScore() + "\n");
                        fw.write(queryDoc.getDocNo() + " " + "0" + " " + ds.getDocName() + " " + i + " " + new DecimalFormat("#0.00000").format(ds.getScore()) + " " + "DEFAULT_LONG" + "\n");

                    }
                }

            }

            fw.close();
            directory.close();
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        long second = (totalTime / 1000) % 60;
        long minute = (totalTime / (1000 * 60)) % 60;
        long hour = (totalTime / (1000 * 60 * 60)) % 24;

        String time = String.format("%02d:%02d:%02d", hour, minute, second);
        System.out.println(time);

    }

    public void parseArguments(String[] arg) {
        luceneUtils.setQueryString(arg[0]);
    }


    public MinMaxPriorityQueue<DocumentTopicsScore> queryIndex(Directory directory, String queryString) {
        IndexReader reader;
        IndexSearcher searcher;

        HashMap<String, Integer> docFreqMap;
        HashMap<String, Float> docLengthMap;
        HashMap<String, Double> docScoreMap;

        MinMaxPriorityQueue<DocumentTopicsScore> documentTopicsScores = null;

        int numberOfDocs = luceneUtils.getDocumentCount(directory);

        try {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);

            Analyzer analyzer = new StandardAnalyzer();

            QueryParser parser = new QueryParser("TEXT", analyzer);
            Query query = parser.parse(QueryParser.escape(queryString));

            // Query Terms
            Set<Term> queryTerms = new LinkedHashSet<Term>();
            searcher.createNormalizedWeight(query, false).extractTerms(queryTerms);

            docFreqMap = luceneUtils.listTermsDocFrequency(reader, queryTerms, query);

            DefaultSimilarity defaultSimilarity = new DefaultSimilarity();

            // Get segments of the index
            List<LeafReaderContext> leafContexts = reader.getContext().reader().leaves();

            docLengthMap = luceneUtils.generateDocLengthMap(defaultSimilarity, searcher, leafContexts);


//            PriorityQueue<DocumentTopicsScore> documentTopicsScores = new PriorityQueue<DocumentTopicsScore>(luceneUtils.CAPACITY, Collections.reverseOrder());
            documentTopicsScores = MinMaxPriorityQueue.create();

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

//                            System.out.println(docName + " :: " + score + " :: " + term.text());
                            docScoreMap.put(docName, score);
                        }
                    }
                }

                Iterator itr = docScoreMap.entrySet().iterator();

                while (itr.hasNext()) {
                    Map.Entry item = (Map.Entry) itr.next();

                    DocumentTopicsScore ds = new DocumentTopicsScore((String) item.getKey(), (Double) item.getValue());

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

class DocumentTopicsScore implements Comparable<DocumentTopicsScore> {

    private String docName;
    private double score;


    DocumentTopicsScore(String docName, double score) {
        this.docName = docName;
        this.score = score;
    }

    @Override
    public int compareTo(DocumentTopicsScore ds) {
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

class TrecTextDocumentUtils {
    public TrecTextDocument parseDocument(String documentString) {
        TrecTextDocument document = new TrecTextDocument();

        String docNo = getElement(documentString, "num", "dom");
        docNo = docNo.replace("Number:", "").trim();
        if (docNo.substring(0, 1).equals("0"))
            docNo = docNo.substring(1);
//        docNo = docNo.substring(11, 13);
        String title = getElement(documentString, "title", "desc");
        title = title.replace("Topic: ", "");
        String desc = getElement(documentString, "desc", "smry");
        desc.replace("Description: ", "");

        document.setDocNo(docNo);
        document.setTitle(title);
        document.setDesc(desc);

        return document;
    }

    public String getElement(String document, String startTag, String endTag) {
        String element = StringUtils.substringBetween(document,
                "<" + startTag + ">", "<" + endTag + ">");
        return element;
    }

    public String[] getElements(String document, String startTag, String endTag) {
        String elements[] = StringUtils.substringsBetween(document, "<" + startTag
                + ">", "<" + endTag + ">");
        return elements;
    }
}

class TrecTextDocument {
    private String docNo;
    private String title;
    private String desc;

    public TrecTextDocument() {

    }

    public String getDocNo() {
        return docNo;
    }

    public void setDocNo(String docNo) {
        this.docNo = docNo;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }
}
