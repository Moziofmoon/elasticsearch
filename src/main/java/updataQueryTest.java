import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Collections;

public class updataQueryTest {

    public static void main(String[] args) throws IOException {
        // 建立client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("47.115.78.71", 9200, "http"),
                        new HttpHost("47.115.78.71", 9201, "http")));

        UpdateByQueryRequest request = new UpdateByQueryRequest("resume_search");

        request.setQuery(new TermQueryBuilder("id", 4));

//        request.setScript(
//                new Script(
//                        ScriptType.INLINE, "painless",
//                        "ctx._source.base.last_job_title = 'hh高级软件工程师'",
//                        Collections.<String, Object>emptyMap()
//                )
//        );
        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "ctx._source.base.graduation_school_name = '台州学院';ctx._source.base.last_job_title = '高级软件工程师'",
                        Collections.<String, Object>emptyMap()
                )
        );

        System.out.println(request);
        BulkByScrollResponse bulkByScrollResponse = client.updateByQuery(request, RequestOptions.DEFAULT);
        System.out.println(bulkByScrollResponse);
        client.close();
    }
}
