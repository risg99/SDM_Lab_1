import pprint
from session_helper_neo4j import create_session

session = create_session()

# Printing query results and summary 
def print_query_results(records, summary):
    pp = pprint.PrettyPrinter(indent = 4)
    
    print("The query `{query}` returned {records_count} records in {time} ms.".format(
        query = summary.query, records_count = len(records),
        time = summary.result_available_after,
    ))

    for record in records:
        pp.pprint(record.data())
        print()

# Query 1: Define research community
def query_define_research_community(session):
    session.run(
        "MATCH (n: ResearchCommunity) DETACH DELETE n"
    )

    session.run(
        """MERGE (rc:ResearchCommunity {name:"Databases"});"""
    )

def query_keywords_belongingTo_community(session):
    result = session.run(
        """MATCH(k:Keyword)
            WHERE k.name IN ['Data Management', 'Indexing', 'Data Modeling', 'Big Data', 'Data Processing', 'Data Storage','Data Querying']
            WITH k
            MATCH(rc:ResearchCommunity {name:"Databases"})
            MERGE(k) - [r1:belongs_to] -> (rc) 
            RETURN k AS keyword, 
                    r1 AS belongs_to, 
                    rc AS researchCommunity
            LIMIT 5;"""
    )
    records = list(result)
    summary = result.consume()
    return records, summary

# Query 2: Identifying papers from conferences or journals belonging to the community
def query_conference_journals_community(session):
    result = session.run(
        """MATCH (rc:ResearchCommunity {name:"Databases"}) <- [:belongs_to] - (k:Keyword) <- [:has*1..] - (cp:Paper) - [r1] -> (x)
            WHERE x:Journal OR x:Conference AND x.title <> 'None' AND r1 IN ['published_in','presented_in']
            WITH cp, COUNT(DISTINCT cp) AS numberOfCommunityPapers
            MATCH (p:Paper) - [r2] -> (x)
            WHERE x:Journal OR x:Conference AND r2 IN ['published_in','presented_in']
            WITH p, cp, x, numberOfCommunityPapers, COUNT(DISTINCT p) AS numberOfPapers
            WHERE (toFloat(numberOfCommunityPapers) / (numberOfPapers)) >= 0.9
            RETURN p.title AS paperName, 
                labels(x)[0] AS confJour, 
                x.name AS nameOfConfJour
            LIMIT 5;"""
    )
    records = list(result)
    summary = result.consume()
    return records, summary

# Identifying the top 100 ranked papers wrt citations from the community
def query_run_pageRank_algorithm(session):
    print('Storing the graph from Part-2 into cypher catalog')
    session.run("""CALL gds.graph.drop('graph1_papers_belongingTo_databases_community',false);""")

    session.run(
        """CALL gds.graph.project.cypher('graph1_papers_belongingTo_databases_community',    
            'MATCH (rc:ResearchCommunity {name:"Databases"}) <- [:belongs_to] - (k:Keyword) <- [:has*1..] - (cp:Paper) - [r1:published_in|presented_in] -> (x)
            WHERE x:Journal OR x:Conference
            WITH cp, COUNT(DISTINCT cp) AS numberOfCommunityPapers
            MATCH (p:Paper) - [r2:published_in|presented_in] -> (x)
            WHERE x:Journal OR x:Conference
            WITH p, cp, x, numberOfCommunityPapers, COUNT(DISTINCT p) AS numberOfPapers
            WHERE (toFloat(numberOfCommunityPapers) / (numberOfPapers)) >= 0.9
            RETURN id(p) AS id;',
            'MATCH (p1:Paper) - [:cites] -> (p2:Paper)
            RETURN id(p1) AS source,
                   id(p2) AS target',           
            {validateRelationships:FALSE});"""
    )

    print('Running the page-rank algorithm for the stored graph')
    session.run(
        """CALL gds.pageRank.write('graph1_papers_belongingTo_databases_community', {
           maxIterations: 20,
           dampingFactor: 0.85,
           writeProperty: 'pagerank'
           });"""
    )

def query_top100_papers_pageRank(session):
    result = session.run(
        """MATCH (rc:ResearchCommunity {name:"Databases"}) <- [:belongs_to] - (k:Keyword) <- [:has*1..] - (cp:Paper) - [r1:published_in|presented_in] -> (x)
            WHERE x:Journal OR x:Conference
            WITH cp, COUNT(DISTINCT cp) AS numberOfCommunityPapers
            MATCH (p:Paper) - [r2:published_in|presented_in] -> (x)
            WHERE x:Journal OR x:Conference
            WITH p, cp, x, numberOfCommunityPapers, COUNT(DISTINCT p) AS numberOfPapers
            WHERE (toFloat(numberOfCommunityPapers) / (numberOfPapers)) >= 0.9
            WITH x, p
            ORDER BY p.pagerank DESC
            WITH x, COLLECT(p) AS p
            RETURN labels(x)[0] AS confJour, x.name AS nameOfConfJour, p[0..100] AS top100Papers
            ORDER BY x
            LIMIT 5;"""
    )
    
    ### Alternative solution
    # result = session.run(
    #     """
    #     MATCH (rc:ResearchCommunity {name:"Databases"}) <- [:belongs_to] - (k:Keyword) <- [:has*1..] - (cp:Paper) - [r1:published_in|presented_in] -> (x)
    #         WHERE x:Journal OR x:Conference
    #         WITH rc, cp, COUNT(DISTINCT cp) AS numberOfCommunityPapers
    #         MATCH (p:Paper) - [r2:published_in|presented_in] -> (x)
    #         WHERE x:Journal OR x:Conference
    #         WITH rc, p, cp, x, numberOfCommunityPapers, COUNT(DISTINCT p) AS numberOfPapers
    #         WHERE (toFloat(numberOfCommunityPapers) / (numberOfPapers)) >= 0.9
    #         WITH rc, p
    #         ORDER BY p.pagerank DESC
    #         WITH rc, COLLECT(DISTINCT p) AS p
    #         RETURN rc.name, p[0..100] AS top100Papers
    #         LIMIT 5;
    #     """
    # )
    
    records = list(result)
    summary = result.consume()
    return records, summary

# Identifying potential reviewers (gurus) who authored atleast 2 of top100 papers from Query 3
def query_gurus_conferences(session):
    result = session.run(
        """MATCH (rc:ResearchCommunity {name:"Databases"}) <- [:belongs_to] - (k:Keyword) <- [:has*1..] - (cp:Paper) - [r1:published_in|presented_in] -> (x)
            WHERE x:Journal OR x:Conference
            WITH cp, COUNT(DISTINCT cp) AS numberOfCommunityPapers
            MATCH (p:Paper) - [r2:published_in|presented_in] -> (x)
            WHERE x:Journal OR x:Conference
            WITH p, cp, x, numberOfCommunityPapers, COUNT(DISTINCT p) AS numberOfPapers
            WHERE (toFloat(numberOfCommunityPapers) / (numberOfPapers)) >= 0.9
            WITH x, p
            ORDER BY p.pagerank DESC
            WITH x, COLLECT(DISTINCT p) AS p
            WITH labels(x)[0] AS confJour, x.name AS nameOfConfJour, p[0..100] AS top100Papers
            UNWIND top100Papers as topPapers
            WITH COLLECT(DISTINCT topPapers) as topPapersList
            MATCH (a:Author) - [r3:writes] -> (p:Paper)
            WHERE p IN topPapersList
            WITH a, COUNT(r3) AS numberOfWrittenPapers
            WHERE numberOfWrittenPapers >= 2
            RETURN a.name AS potentialReviewerName,
                a.email AS potentialReviewerEmail,
                numberOfWrittenPapers AS guru
            LIMIT 5;"""
    )
    
    ### Alternative Solution
    # result = session.run(
    #     """
    #     MATCH (rc:ResearchCommunity {name:"Databases"}) <- [:belongs_to] - (k:Keyword) <- [:has*1..] - (cp:Paper) - [r1:published_in|presented_in] -> (x)
    #         WHERE x:Journal OR x:Conference
    #         WITH rc, cp, COUNT(DISTINCT cp) AS numberOfCommunityPapers
    #         MATCH (p:Paper) - [r2:published_in|presented_in] -> (x)
    #         WHERE x:Journal OR x:Conference
    #         WITH rc, p, cp, x, numberOfCommunityPapers, COUNT(DISTINCT p) AS numberOfPapers
    #         WHERE (toFloat(numberOfCommunityPapers) / (numberOfPapers)) >= 0.9
    #         WITH rc, p
    #         ORDER BY p.pagerank DESC
    #         WITH rc, COLLECT(DISTINCT p) AS p
    #         WITH rc.name as communityName , p[0..100] AS top100Papers
    #         UNWIND top100Papers as topPapers
    #         WITH COLLECT(DISTINCT topPapers) as topPapersList
    #         MATCH (a:Author) - [r3:writes] -> (p:Paper)
    #         WHERE p IN topPapersList
    #         WITH a, COUNT(DISTINCT r3) AS numberOfWrittenPapers
    #         WHERE numberOfWrittenPapers >= 2
    #         RETURN a.name AS potentialReviewerName,
    #             a.email AS potentialReviewerEmail,
    #             numberOfWrittenPapers AS guru
    #         LIMIT 5;
    #     """
    # )
    
    records = list(result)
    summary = result.consume()
    return records, summary


print('Part 1..........')    
print('Creating Research Community node with name = Databases')
session.execute_write(query_define_research_community)
print('Creating relations from keywords to the Research Community')
records, summary = session.execute_write(query_keywords_belongingTo_community)
print_query_results(records, summary)


print('Part 2..........')
print('Finding papers from conferences or journals that belong to research community of databases')
records, summary = session.execute_read(query_conference_journals_community)
print_query_results(records, summary)

print('Part 3..........')
session.execute_write(query_run_pageRank_algorithm)
print('Obtaining the results of page-rank')
records, summary = session.execute_read(query_top100_papers_pageRank)
print_query_results(records, summary)

print('Part 4..........')
print('Finding gurus of the top conferences and journals')
records, summary = session.execute_read(query_gurus_conferences)
print_query_results(records, summary)

session.close()