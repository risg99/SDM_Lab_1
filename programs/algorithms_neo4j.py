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

# Algorithm 1: Node Similarity
# Use Case: How similar are two papers based on their keywords

def query_simulate_node_similarity_algorithm(session):
    print('Dropping the graph from cypher catalog, only if exists')
    session.run("""CALL gds.graph.drop('myGraph1',false);""")

    print('Project the graph')
    session.run(
        """CALL gds.graph.project('myGraph1', ['Paper','Keyword'], 'has');"""
    )

    print('Simulating the node similarity algorithm for the stored graph')
    session.run(
        """CALL gds.nodeSimilarity.write.estimate('myGraph1', {
              writeRelationshipType: 'SIMILAR',
              writeProperty: 'score'
            });"""
    )

    print('Running the node similarity algorithm for the stored graph')
    result = session.run(
        """CALL gds.nodeSimilarity.stream('myGraph1')
            YIELD node1, node2, similarity
            RETURN gds.util.asNode(node1).title AS Paper1, 
                gds.util.asNode(node2).title AS Paper2, 
                similarity
            ORDER BY similarity, Paper1, Paper2
            LIMIT 5;"""
    )
    records = list(result)
    summary = result.consume()
    return records, summary



# Alternative test case: testing the similarity algorithm from scratch
# def query_analyse_paper_similarity(session):
#     result = session.run(
#         """ MATCH (p1:Paper) - [r1:has] -> (k1:Keyword), (p2:Paper) - [r2:has] -> (k2:Keyword)
#             WITH p1, k1, p2, k2
#             ORDER BY p1.score DESC
#             WITH p1, p2, k1, k2
#             WHERE p1.title <> p2.title
#             RETURN p1.title AS paper1, p2.title AS paper2, k1, k2
#             ORDER BY p1
#             LIMIT 5;"""
#     )
#     records = list(result)
#     summary = result.consume()
#     return records, summary

# Corresponding function calls
# print('Obtaining the results')
# records, summary = session.execute_read(query_analyse_paper_similarity)
# print_query_results(records, summary)



# Algorithm 2: Betweeness Centrality
# Use Case: Measure the importance of paper using the betweeness centrality algorithm

def query_simulate_betweeneness_centrality_algorithm(session):
    print('Dropping the graph from cypher catalog, only if exists')
    session.run("""CALL gds.graph.drop('myGraph2',false);""")

    print('Projecting the graph')
    session.run(
        """CALL gds.graph.project('myGraph2', 'Paper', {cites: {orientation: 'UNDIRECTED'}});"""
    )

    print('Simulating the node similarity algorithm for the stored graph')
    session.run(
        """CALL gds.betweenness.write.estimate('myGraph2', 
            { writeProperty: 'betweenness' });"""
    )

    print('Running the node similarity algorithm for the stored graph')
    result = session.run(
        """CALL gds.betweenness.stream('myGraph2')
            YIELD nodeId, score
            RETURN gds.util.asNode(nodeId).title AS title, 
            score
            ORDER BY score, title DESC
            LIMIT 5;"""
    )
    records = list(result)
    summary = result.consume()
    return records, summary



print('Algorithm 1 - Node Similarity..........')
records, summary = session.execute_read(query_simulate_node_similarity_algorithm)
print_query_results(records, summary)


print('Algorithm 2 - Betweenness Centrality..........')
records, summary = session.execute_read(query_simulate_betweeneness_centrality_algorithm)
print_query_results(records, summary)

session.close()