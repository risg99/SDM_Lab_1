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


# Query 1: Top 3 most cited papers per conference
def query_top3_cited_papers_conference(session):
    result = session.run(
        """MATCH (p:Paper) - [r1:cites] -> (citedPaper:Paper) - [r2:presented_in] -> (c:Conference) 
            WITH c, citedPaper, count(r1) AS numberOfCitations
            ORDER BY c, numberOfCitations DESC
            WITH c, COLLECT(citedPaper)[0..3] AS top3
            WHERE top3[1].title <> 'None' AND top3[2].title <> 'None'
            RETURN c.name as conference, 
                   top3[0].title AS topCitedPaper1,
                   top3[1].title AS topCitedPaper2,
                   top3[2].title AS topCitedPaper3
            ;"""
        )
    records = list(result)
    summary = result.consume()
    return records, summary

# Query 2: Authors that have published in the same conference in at least 4 editions
def query_authors_published_same_conference_4editions(session):
    result = session.run(
        """MATCH (a:Author) - [:writes] -> (p:Paper) - [:presented_in] -> (c:Conference)
            WITH c.name as conferenceName, a, COUNT(DISTINCT c.edition) AS distinctEditions
            WHERE distinctEditions >= 4
            WITH conferenceName, a
            MATCH (a2:Author) - [:writes] -> (p2:Paper) - [:presented_in] -> (c)
            WHERE a2.ID <> a.ID
            WITH conferenceName, a, a2, COUNT(DISTINCT c.edition) AS distinctEditions1
            WHERE distinctEditions1 >= 4
            RETURN conferenceName,
                   a.name as author1_name,
                   a.email as author1_email,
                   a2.name as author2_name,
                   a2.email as author2_email
            LIMIT 5;"""
    )
    
    ### Alternative Solution
    # result = session.run(
    #     """
    #     MATCH (a:Author) - [:writes] -> (p:Paper) - [:presented_in] -> (c:Conference)
    #         WITH c.name as conferenceName, a, COUNT(DISTINCT c.edition) AS distinctEditions
    #         WHERE distinctEditions >= 4
    #         WITH conferenceName, a
    #         RETURN conferenceName, COLLECT(a.name) as community
    #         LIMIT 5;
    #     """
    # )
    
    records = list(result)
    summary = result.consume()
    return records, summary

# Query 3 Impact factor
# Impact factor = Citations(year1) / Publications(year1) + Publications(year2)
def query_impact_factor(session):
    result = session.run(
        """MATCH(p:Paper) - [r1:cites] -> (citedP:Paper) - [r2:published_in] -> (j:Journal) 
            WITH j, r2.year as currYear, COUNT(r1) AS totalCitations
            MATCH (p2:Paper) - [r3:published_in] -> (j)
            WHERE r3.year = currYear - 1 OR r3.year = currYear - 2
            WITH j, currYear, totalCitations, COUNT(r3) AS totalPublications
            WHERE totalPublications > 0
            RETURN j.name AS journalName,
                    currYear AS yearOfPublication,
                    toFloat(totalCitations)/totalPublications AS impactFactor
            ORDER BY impactFactor DESC
            LIMIT 5;"""
    )
    records = list(result)
    summary = result.consume()
    return records, summary

# Query 4 H-Index ----- getting op of 3k+ rows
# H-Index = atleast h publications have h citations

def query_h_index(session):
    result = session.run(
        """MATCH(a:Author) - [r1:writes] -> (p1:Paper) - [r2:cites] -> (p2:Paper)
            WITH a, p2, COLLECT(p1) as papers
            WITH a, p2, RANGE(1, SIZE(papers)) AS listOfPapers
            UNWIND listOfPapers AS lp
            WITH a, lp AS currHIndex, count(p2) AS citedPapers
            WHERE currHIndex <= citedPapers
            RETURN a.name AS authorName,
                   currHIndex as hIndex
            ORDER BY currHIndex DESC
            LIMIT 5;"""
    )
    records = list(result)
    summary = result.consume()
    return records, summary


records, summary = session.execute_read(query_top3_cited_papers_conference)
print('Query Result 1.........')
print_query_results(records, summary)

records, summary = session.execute_read(query_authors_published_same_conference_4editions)
print('Query Result 2.........')
print_query_results(records, summary)

records, summary = session.execute_read(query_impact_factor)
print('Query Result 3.........')
print_query_results(records, summary)

records, summary = session.execute_read(query_h_index)
print('Query Result 4.........')
print_query_results(records, summary)

session.close()