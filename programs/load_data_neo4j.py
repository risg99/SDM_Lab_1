from session_helper_neo4j import create_session, clean_session
from drop_create_indexes_neo4j import drop_indexes, create_indexes

def load_node_keyword_semantic(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///keywords_semantic.csv' AS line
            CREATE (:Keyword {
                ID: line.ID,
                name: line.name,
                domain: line.domain
        })"""
    )

def load_node_author_semantic(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///authors_semantic.csv' AS line
            CREATE (:Author {
                ID: line.ID,
                name: line.name,
                email: line.email
        })"""
    )

def load_node_conference_semantic(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///conference_semantic.csv' AS line
            CREATE (:Conference {
                ID: line.ID,
                name: line.name,
                year: toInteger(line.year),
                edition: toInteger(line.edition)
        })"""
    )

def load_node_journal_semantic(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///journal_semantic.csv' AS line
            CREATE (:Journal {
                ID: line.ID,
                name: line.name
        })"""
    )

def load_node_proceeding_semantic(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///proceedings_semantic.csv' AS line
            CREATE (:Proceeding {
                ID: line.ID,
                name: line.name,
                city: line.city
        })"""
    )

def load_node_paper_semantic(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///papers_semantic.csv' AS line
            CREATE (:Paper {
                ID: line.ID,
                title: line.title,
                abstract: line.abstract,
                pages: line.pages,
                doi: line.doi,
                link: line.link
        })"""
    )

def load_relation_conference_ispart_proceeding(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///conference_part_of_proceedings.csv' AS line
            MATCH (conf:Conference {ID: line.START_ID})
            WITH conf, line
            MATCH (proc:Proceeding {ID: line.END_ID})
            CREATE (conf)-[:is_part]->(proc)"""
    )

def load_relation_author_writes_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///author_writes_papers.csv' AS line
            MATCH (author:Author {ID: line.START_ID})
            WITH author, line
            MATCH (paper:Paper {ID: line.END_ID})
            CREATE (author)-[w:writes]->(paper)
            SET w.corresponding_author = toBoolean(line.corresponding_author)"""
    )

def load_relation_paper_has_keyword(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///paper_has_keywords.csv' AS line
            MATCH (paper:Paper {ID: line.START_ID})
            WITH paper, line
            MATCH (keyword:Keyword {ID: line.END_ID})
            CREATE (paper) - [:has] -> (keyword)"""
    )

def load_relation_paper_presentedin_conference(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///paper_presented_in_conference.csv' AS line
            MATCH (paper:Paper {ID: line.START_ID})
            WITH paper, line
            MATCH (conf:Conference {ID: line.END_ID})
            CREATE (paper) - [:presented_in] -> (conf)"""
    )

def load_relation_paper_publishedin_journal(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///paper_published_in_journal.csv' AS line
            MATCH (paper:Paper {ID: line.START_ID})
            WITH paper, line
            MATCH (jour:Journal {ID: line.END_ID})
            CREATE (paper) - [r:published_in] -> (jour)
            SET r.volume = toInteger(line.volume), r.year = toInteger(line.year)"""
    )

def load_relation_paper_cites_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///paper_cites_paper.csv' AS line
            MATCH (paper:Paper {ID: line.START_ID})
            WITH paper, line
            MATCH (citedPaper:Paper {ID: line.END_ID})
            CREATE (paper) - [:cites] -> (citedPaper)"""
    )

def load_relation_author_reviews_paper(session):
    session.run(
        """LOAD CSV WITH HEADERS FROM 'file:///author_review_papers.csv' AS line
            MATCH (author:Author {ID: line.START_ID})
            WITH author, line
            MATCH (paper:Paper {ID: line.END_ID})
            CREATE (author) - [:reviews] -> (paper)"""
    )

session = create_session()
session = clean_session(session)

print('Dropping the indexes created for the nodes and relations in the database...')
session.execute_write(create_indexes)

print('Creating and loading the nodes and relations into the database...')
session.execute_write(load_node_keyword_semantic)
session.execute_write(load_node_author_semantic)
session.execute_write(load_node_conference_semantic)
session.execute_write(load_node_journal_semantic)
session.execute_write(load_node_proceeding_semantic)
session.execute_write(load_node_paper_semantic)
session.execute_write(load_relation_conference_ispart_proceeding)
session.execute_write(load_relation_author_writes_paper)
session.execute_write(load_relation_paper_has_keyword)
session.execute_write(load_relation_paper_presentedin_conference)
session.execute_write(load_relation_paper_publishedin_journal)
session.execute_write(load_relation_paper_cites_paper)
session.execute_write(load_relation_author_reviews_paper)
print('Creation and loading done for the database.')

print('Creating the indexes for the nodes and relations in the database...')
session.execute_write(create_indexes)

session.close()