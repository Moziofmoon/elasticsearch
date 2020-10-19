import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

public class Test {

    public static void main(String[] args) throws IOException {

        // 建立client
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("47.115.78.71", 9200, "http"),
                        new HttpHost("47.115.78.71", 9201, "http")));
        //创建resquest
        SearchRequest searchRequest = new SearchRequest();
        //all match
//        allMatchSearch(searchRequest);
        //职位检索
//        jobHunterSearch(searchRequest);
        //简历搜索
//        resumeHunterSearch(searchRequest);
        //简历搜索resume_es
//        resumeEsSearch(searchRequest);
        //测试上传数据
        testCustomer(searchRequest);
        // 测试修改数据
//        testUpdate(searchRequest);
        // 创建response
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("=================request======================");
        System.out.println(searchResponse);
        client.close();
    }

    private static void testUpdate(SearchRequest searchRequest) {
        //1.设定索引
        searchRequest.indices("resume_search");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermQueryBuilder idQuery = QueryBuilders.termQuery("id", 4);
        searchSourceBuilder.query(idQuery);
        String[] includes = {"base.last_job_title","base.graduation_school_name"};
        String[] excludes = {};
        searchSourceBuilder.fetchSource(includes, excludes);
        searchRequest.source(searchSourceBuilder);

    }

    private static void testCustomer(SearchRequest searchRequest) {
        //1.设定索引
        searchRequest.indices("resumes_search");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //2.定义query
//        MatchAllQueryBuilder query = QueryBuilders.matchAllQuery();
        //关键词 全文检索
        MatchQueryBuilder contextQuery = QueryBuilders.matchQuery("context", "东方明珠");
//        QueryBuilder contextQuery = chineseAndPinYinSearch("context", "技术总监");
        // 当前职位 last_job_title
        MatchQueryBuilder titleQuery = QueryBuilders.matchQuery("base.last_job_title", "高级项目经理");
//        QueryBuilder titleQuery = chineseAndPinYinSearch("base.last_job_title", "财务");
        // 当前行业 current_industry
        MatchQueryBuilder industryQuery = QueryBuilders.matchQuery("base.current_industry", "计算机软件");

        // 学历 highest_education
        RangeQueryBuilder educationQuery = QueryBuilders.rangeQuery("base.highest_education").gte(3);
        // 所在城市 current_location
        MatchQueryBuilder cityQuery = QueryBuilders.matchQuery("base.current_location", "北京");
        // 公司 last_company
        MatchQueryBuilder baseCompanyQuery = QueryBuilders.matchQuery("base.last_company", "东方明珠");
        MatchQueryBuilder workCompanyQuery = QueryBuilders.matchQuery("works.company_name", "东方明珠");
        // 专业名称 graduation_major_name
        MatchQueryBuilder baseProfessionalName = QueryBuilders.matchQuery("base.graduation_major_name", "信息与计算科学");
        MatchQueryBuilder eduProfessionName = QueryBuilders.matchQuery("educations.major_name", "计算科学");
        // 性别 sex 1 男 2 女
        TermQueryBuilder sexQuery = QueryBuilders.termQuery("base.sex", 1);
        // 年龄 age
        RangeQueryBuilder ageQuery = QueryBuilders.rangeQuery("base.age")
                .gt(20)
                .lte(50)
                ;
        // 工作年限
        RangeQueryBuilder workingAgeQuery = QueryBuilders.rangeQuery("base.working_age")
                .gte(0)
                .lte(100);
        // 年薪 salary
        RangeQueryBuilder salaryQuery = QueryBuilders.rangeQuery("base.current_salary").gte(20).lte(50);
        // 期望年薪 expect_salary
        RangeQueryBuilder expectSalaryQuery = QueryBuilders.rangeQuery("base.expect_salary").gte(30).lte(80);
        RangeQueryBuilder minExpectSalaryQuery = QueryBuilders.rangeQuery("base.expect_salary_min").gte(0);
        RangeQueryBuilder maxExpectSalaryQuery = QueryBuilders.rangeQuery("base.expect_salary_max").lte(100);
        // 企业id
        TermQueryBuilder enterpriseIdQuery = QueryBuilders.termQuery("enterprise_id", 2);
        //3.定义bool查询
        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchAllQuery())
//                .must(contextQuery)
//                .must(titleQuery)
//                .must(industryQuery)
                .must(educationQuery)
//                .must(cityQuery)
//                .must(baseCompanyQuery)
//                .must(workCompanyQuery)
//                .must(baseProfessionalName)
//                .must(sexQuery)
//                .must(eduProfessionName)
//                .must(ageQuery)
//                .must(salaryQuery)
//                .must(expectSalaryQuery)
//                .must(workingAgeQuery)
//                .must(minExpectSalaryQuery)
//                .must(maxExpectSalaryQuery)
//                .must(enterpriseIdQuery)

                ;

        // 设置sort
        searchSourceBuilder.sort("base.highest_education", SortOrder.ASC);
        //4.添加query
        searchSourceBuilder.query(query);
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        String[] includes = {"base.name", "base.highest_education"};
        String[] excludes = {};
        searchSourceBuilder.fetchSource(includes, excludes);

        //5.分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        //6.添加request
        searchRequest.source(searchSourceBuilder);


        System.out.println("=================source======================");
        System.out.println(searchSourceBuilder);
    }

    private static void resumeEsSearch(SearchRequest searchRequest) {
        //1.设定索引
        searchRequest.indices("resume_es");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //2.定义query
        MatchQueryBuilder resume = QueryBuilders.matchQuery("resume_context", "京东商城 客服 北京");
        MatchQueryBuilder resume_must = QueryBuilders.matchQuery("resume_context", "石湫");
        MatchQueryBuilder resume_should = QueryBuilders.matchQuery("resume_context", "外包");
        //3.定义bool查询
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(resume).must(resume_must).should(resume_should);
        //4.添加query
        SearchSourceBuilder source = searchSourceBuilder.query(query);
        //5.分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        //6.添加request
        searchRequest.source(source);

        System.out.println("=================source======================");
        System.out.println(source);
    }

    private static void jobHunterSearch(SearchRequest searchRequest) {
        searchRequest.indices("job_hunter");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // query
        TermQueryBuilder function = QueryBuilders.termQuery("function.keyword", "人力资源");
        TermQueryBuilder job = QueryBuilders.termQuery("job_title.keyword", "产品经理");
        TermQueryBuilder name = QueryBuilders.termQuery("performer.keyword", "张千雪");
        TermQueryBuilder headCount = QueryBuilders.termQuery("head_count", 11);
        TermQueryBuilder city = QueryBuilders.termQuery("city.keyword", "北京");
        TermQueryBuilder status = QueryBuilders.termQuery("status.keyword", "招聘中");
        TermQueryBuilder priority = QueryBuilders.termQuery("priority", 4);
        TermQueryBuilder applys = QueryBuilders.termQuery("applys", 95);
        TermQueryBuilder selecteds = QueryBuilders.termQuery("selecteds", 52);
        TermQueryBuilder recommendeds = QueryBuilders.termQuery("recommendeds", 40);
        TermQueryBuilder interviews = QueryBuilders.termQuery("interviews", 16);
        TermQueryBuilder hires = QueryBuilders.termQuery("hires", 5);
        TermQueryBuilder entrys = QueryBuilders.termQuery("entrys", 2);
        TermQueryBuilder updateAt = QueryBuilders.termQuery("update_at", "2019-05-27");



        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(function)
                .must(job)
                .must(name)
                .must(city)
                .must(status)
                .must(priority)
                .must(headCount)
                .must(applys)
                .must(selecteds)
                .must(recommendeds)
                .must(interviews)
                .must(hires)
                .must(entrys)
                .must(updateAt)
                ;
        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder);
        // 排序
        searchSourceBuilder.sort("priority", SortOrder.DESC);
        searchSourceBuilder.sort("update_at", SortOrder.DESC);
        //分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchRequest.source(query);



        System.out.println(searchSourceBuilder);
    }
    private static void resumeHunterSearch(SearchRequest searchRequest) {
        searchRequest.indices("resume_hunter_chinese");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // query
        TermQueryBuilder industry = QueryBuilders.termQuery("industry.keyword", "电子商务");
        TermQueryBuilder function = QueryBuilders.termQuery("function.keyword", "人力资源");
        TermQueryBuilder currentAddress = QueryBuilders.termQuery("current_address.keyword", "北京");
        TermQueryBuilder label = QueryBuilders.termQuery("label.keyword", "高质量");
        TermQueryBuilder lastJobTitle = QueryBuilders.termQuery("last_job_title.keyword", "产品经理");
        TermQueryBuilder category = QueryBuilders.termQuery("category", 2);
        TermQueryBuilder name = QueryBuilders.termQuery("name.keyword", "张千雪");
        TermQueryBuilder phone = QueryBuilders.termQuery("phone", "18999999283");
        TermQueryBuilder status = QueryBuilders.termQuery("status.keyword", "应聘");

        RangeQueryBuilder highestEducation = QueryBuilders.rangeQuery("highest_education").gte(2);
        RangeQueryBuilder age = QueryBuilders.rangeQuery("age").from(10).to(40);
        RangeQueryBuilder workAge = QueryBuilders.rangeQuery("working_age").from(1).to(10);


        QueryBuilder remark = chineseAndPinYinSearch("remark","犹豫");
        QueryBuilder lastCompany = chineseAndPinYinSearch("last_company","阿里");





        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                .must(industry)
                .must(function)
                .must(currentAddress)
                .must(label)
                .must(category)
                .must(name)
                .must(phone)
                .must(lastJobTitle)
                .must(status)
                .should(remark)
                .must(age)
                .must(workAge)
                .must(highestEducation)
                .must(lastCompany)
                ;
        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder);
        // 排序
//        searchSourceBuilder.sort("priority", SortOrder.DESC);
//        searchSourceBuilder.sort("update_time", SortOrder.DESC);
        //分页
//        searchSourceBuilder.from(0);
//        searchSourceBuilder.size(10);
        HighlightBuilder highlightBuilder = new HighlightBuilder(); //生成高亮查询器
        highlightBuilder.field("status");      //高亮查询字段
//        highlightBuilder.field("job_title");    //高亮查询字段
//        highlightBuilder.requireFieldMatch(false);     //如果要多个字段高亮,这项要为false
//        highlightBuilder.preTags("<span style=\"color:red\">");   //高亮设置
//        highlightBuilder.postTags("</span>");

        //下面这两项,如果你要高亮如文字内容等有很多字的字段,必须配置,不然会导致高亮不全,文章内容缺失等
//        highlightBuilder.fragmentSize(800000); //最大高亮分片数
//        highlightBuilder.numOfFragments(0); //从第一个分片获取高亮片段

        query.highlighter(highlightBuilder);
        searchRequest.source(query);
        System.out.println(searchSourceBuilder);
    }

    private static void allMatchSearch(SearchRequest searchRequest) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        System.out.println(searchRequest);
        System.out.println(searchSourceBuilder);

    }


    //中文、拼音混合搜索
    private static QueryBuilder chineseAndPinYinSearch(String field,String words){

        //使用dis_max直接取多个query中，分数最高的那一个query的分数即可
        DisMaxQueryBuilder disMaxQueryBuilder=QueryBuilders.disMaxQuery();

        /**
         * 纯中文搜索，不做拼音转换,采用edge_ngram分词(优先级最高)
         * 权重* 5
         */
        QueryBuilder normSearchBuilder=QueryBuilders.matchQuery(field+".ngram",words).analyzer("ngramSearchAnalyzer").boost(5f);

        /**
         * 拼音简写搜索
         * 1、分析key，转换为简写  case:  南京东路==>njdl，南京dl==>njdl，njdl==>njdl
         * 2、搜索匹配，必须完整匹配简写词干
         * 3、如果有中文前缀，则排序优先
         * 权重*1
         */

        TermQueryBuilder pingYinSampleQueryBuilder = QueryBuilders.termQuery(field+".SPY", words);

        /**
         * 拼音简写包含匹配，如 njdl可以查出 "城市公牛 南京东路店"，虽然非南京东路开头
         * 权重*0.8
         */
        QueryBuilder  pingYinSampleContainQueryBuilder=null;
        if(words.length()>1){
            pingYinSampleContainQueryBuilder=QueryBuilders.wildcardQuery(field+".SPY", "*"+words+"*").boost(0.8f);
        }

        /**
         * 拼音全拼搜索
         * 1、分析key，获取拼音词干   case :  南京东路==>[nan,jing,dong,lu]，南京donglu==>[nan,jing,dong,lu]
         * 2、搜索查询，必须匹配所有拼音词，如南京东路，则nan,jing,dong,lu四个词干必须完全匹配
         * 3、如果有中文前缀，则排序优先
         * 权重*1
         */
        QueryBuilder pingYinFullQueryBuilder=null;
        if(words.length()>1){
            pingYinFullQueryBuilder=QueryBuilders.matchPhraseQuery(field+".FPY", words).analyzer("pinyiFullSearchAnalyzer");
        }

        /**
         * 完整包含关键字查询(优先级最低，只有以上四种方式查询无结果时才考虑）
         * 权重*0.8
         */
        QueryBuilder containSearchBuilder=QueryBuilders.matchQuery(field, words).analyzer("ikSearchAnalyzer").minimumShouldMatch("100%");

        disMaxQueryBuilder
                .add(normSearchBuilder)
                .add(pingYinSampleQueryBuilder)
                .add(containSearchBuilder);

        //以下两个对性能有一定的影响，故作此判定，单个字符不执行此类搜索
        if(pingYinFullQueryBuilder!=null){
            disMaxQueryBuilder.add(pingYinFullQueryBuilder);
        }
        if(pingYinSampleContainQueryBuilder!=null){
            disMaxQueryBuilder.add(pingYinSampleContainQueryBuilder);
        }

        return disMaxQueryBuilder;
    }



}
