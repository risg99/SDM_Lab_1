from session_helper_neo4j import create_session, clean_session

def load_node_institution_semantic(session):
    session.run("""LOAD CSV WITH HEADERS FROM 'file:///authors_semantic.csv' AS line
        MERGE(i:Institution {
            name: line.institution
        });""")

    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///authors_semantic.csv' AS line
            CREATE (a:Author {name:line.name}) 
            WITH a, line
            MATCH (i:Institution {name: line.institution})
            WITH a, i, line
            CREATE (a) - [r:is_from] -> (i)
            SET r.department = line.department;"""
    )

# To identify the decision on the basis of acceptance probability, we are assigning probability values
# given by each author to their to-be-reviewed paper. 
# We have assumed that acceptanceProbability is an temporary attribute (not to be included) in final graph
# as it's finally evolved into decision.
def load_relation_author_reviews_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///author_review_papers.csv' AS line
            MATCH (author:Author {ID: line.START_ID}) - [r:reviews] -> (paper:Paper {ID: line.END_ID})
            SET r.comment = line.comment, r.acceptanceProbability = toFloat(line.acceptanceProbability);"""
    )

# In this case, we are assuming each reviewer assigns a particular probability / score out of 10 (each)
# It is accepted when the probability > 0.5 or score > 15
def query_accept_paper_publication(session):
    session.run(
        """MATCH (a:Author) - [r:reviews] -> (p:Paper)
            WITH a, r, p.name AS paperTitle
            CASE 
                WHEN SUM(r.acceptanceProbability) > 0.5 
                    THEN True
                ELSE 
                    False
            END AS decision
            SET r.decision = decision;"""
    )

session = create_session()

print('Creating and loading the evolved nodes and relations into the database...')
session.execute_write(load_node_institution_semantic)
session.execute_write(load_relation_author_reviews_paper)
print('Creation and loading for evolved nodes done for the database.')

session.close()