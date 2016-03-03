import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.*;

public class CompareAlgorithms {

    private String queryString;
    private static CompareAlgorithms luceneUtils = new CompareAlgorithms();
    private static TrecTextDocumentUtils1 utils = new TrecTextDocumentUtils1();

    public static final String INDEX_DIR = "index/";
    public static final String TOPICS_FILE = "topics.51-100";
    public static final int CAPACITY = 1000;

    public CompareAlgorithms() {
    }

    public static void main(final String a[]) {
        File dirToIndex;
        FileWriter fw;
        Directory directory;
        MinMaxPriorityQueue<DocumentTopicsScore1> documentTopicsScores;
        HashMap<String, Similarity> similarityMap = new HashMap<String, Similarity>();

        similarityMap.put("Default", new DefaultSimilarity());
        similarityMap.put("BM25", new BM25Similarity());
        similarityMap.put("LMDirichlet", new LMDirichletSimilarity());
        similarityMap.put("LMJelinekMercer", new LMJelinekMercerSimilarity((float) 0.7));

        try {
            File fileToQuery = new File(TOPICS_FILE);

            String fileString = FileUtils.readFileToString(fileToQuery);
            String[] docElements = utils.getElements(fileString, "top", "/top");

            directory = FSDirectory.open(Paths.get(INDEX_DIR));

            IndexReader reader = DirectoryReader.open(directory);
            IndexSearcher searcher = new IndexSearcher(reader);

            Iterator itr = similarityMap.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry item = (Map.Entry) itr.next();

                fw = new FileWriter(item.getKey()+"SHORTQUERY.txt");
                for (String docElement : docElements) {
                    TrecTextDocument1 queryDoc = utils.parseDocument(docElement);

//                    fw.write((String) item.getKey() + "\n\n");
//                    fw.write("====================================");
                    ScoreDoc[] hits = luceneUtils.queryIndex(directory, queryDoc.getTitle(), (Similarity) item.getValue());
//                    fw.write("Query No | Doc-ID        | Rank | Type      | Score" + "\n");
//                    fw.write("--------------------------------------------------------------" + "\n");

                    for (int i = 0; i < hits.length; i++) {
                        Document doc = searcher.doc(hits[i].doc);
                        fw.write(queryDoc.getDocNo() + " " + "0" + " " + doc.get("DOCNO") + " " + i + " " + new DecimalFormat("#0.00000").format(hits[i].score) + " " + item.getKey() + "_SHORT" + "\n");

//                        fw.write(queryDoc.getDocNo() + "     |" + doc.get("DOCNO") + " | " + i + "    | BM25_short | " + hits[i].score + "\n");

                    }
                }
                fw.close();
            }

            itr = similarityMap.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry item = (Map.Entry) itr.next();

                fw = new FileWriter(item.getKey()+"LONGQUERY.txt");
                for (String docElement : docElements) {
                    TrecTextDocument1 queryDoc = utils.parseDocument(docElement);

//                    fw.write((String) item.getKey() + "\n\n");
//                    fw.write("====================================");
                    ScoreDoc[] hits = luceneUtils.queryIndex(directory, queryDoc.getDesc(), (Similarity) item.getValue());
//                    fw.write("Query No | Doc-ID        | Rank | Type      | Score" + "\n");
//                    fw.write("--------------------------------------------------------------" + "\n");

                    for (int i = 0; i < hits.length; i++) {
                        Document doc = searcher.doc(hits[i].doc);

                        fw.write(queryDoc.getDocNo() + " " + "0" + " " + doc.get("DOCNO") + " " + i + " " + new DecimalFormat("#0.00000").format(hits[i].score) + " " + item.getKey() + "_LONG" + "\n");

//                        fw.write(queryDoc.getDocNo() + "     |" + doc.get("DOCNO") + " | " + i + "    | BM25_short | " + hits[i].score + "\n");

                    }
                }
                fw.close();
            }
            directory.close();
        } catch (final IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public ScoreDoc[] queryIndex(Directory directory, String queryString, Similarity similarity) {
        IndexReader reader;
        IndexSearcher searcher = null;
        Analyzer analyzer;

        try {
            reader = DirectoryReader.open(directory);
            searcher = new IndexSearcher(reader);
            analyzer = new StandardAnalyzer();
            searcher.setSimilarity(similarity);

            QueryParser parser = new QueryParser("TEXT", analyzer);
            Query query = parser.parse(QueryParser.escape(queryString));

            TopDocs results = searcher.search(query, 1000);
            ScoreDoc[] hits = results.scoreDocs;

            return hits;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}

class DocumentTopicsScore1 implements Comparable<DocumentTopicsScore1> {

    private String docName;
    private double score;


    DocumentTopicsScore1(String docName, double score) {
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

class TrecTextDocumentUtils1 {
    public TrecTextDocument1 parseDocument(String documentString) {
        TrecTextDocument1 document = new TrecTextDocument1();

        String docNo = getElement(documentString, "num", "dom");
        docNo = docNo.replace("Number:", "").trim();
        if (docNo.substring(0, 1).equals("0"))
            docNo = docNo.substring(1);
        String title = getElement(documentString, "title", "desc");
        title = title.replace("Topic: ", "");
        String desc = getElement(documentString, "desc", "smry");
        desc.replace("   sDescription: ", "");
        System.out.println(desc);

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

class TrecTextDocument1 {
    private String docNo;
    private String title;
    private String desc;

    public TrecTextDocument1() {

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
