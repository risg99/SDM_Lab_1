from session_helper_neo4j import create_session

def drop_indexes(session):
    session.run("""
        DROP INDEX idx_paper IF EXISTS; 
        DROP INDEX idx_author IF EXISTS;
        DROP INDEX idx_journal IF EXISTS;
        DROP INDEX idx_conference IF EXISTS;
        DROP INDEX idx_proceeding IF EXISTS;
        DROP INDEX idx_keyword IF EXISTS;"""
    )

def create_indexes(session):
    print('Index for Paper')
    session.run("""CREATE INDEX idx_paper IF NOT EXISTS
        FOR (p:Paper) 
        ON (p.ID);"""
    )

    print('Index for Keyword')
    session.run("""CREATE INDEX idx_keyword IF NOT EXISTS
        FOR (k:Keyword) 
        ON (k.keyword);"""
    )

    print('Index for Author')
    session.run("""CREATE INDEX idx_author IF NOT EXISTS
        FOR (a:Author) 
        ON (a.ID);"""
    )

    print('Index for Conference')
    session.run("""CREATE INDEX idx_conference IF NOT EXISTS
        FOR (c:Conference) 
        ON (c.ID);"""
    )

    print('Index for Journal')
    session.run("""CREATE INDEX idx_journal IF NOT EXISTS 
        FOR (j:Journal) 
        ON (j.ID);"""
    )

    print('Index for Proceeding')
    session.run("""CREATE INDEX idx_proceeding IF NOT EXISTS 
        FOR (p:Proceeding) 
        ON (p.ID);"""
    )

session = create_session()

session.close()