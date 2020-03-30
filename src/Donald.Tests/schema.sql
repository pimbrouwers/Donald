DROP TABLE IF EXISTS author;

CREATE TABLE author (    
    author_id   INTEGER   NOT NULL  PRIMARY KEY  AUTOINCREMENT  
  , full_name   TEXT      NOT NULL  UNIQUE);

INSERT INTO author (full_name) 
VALUES
    ('Pim Brouwers')
  , ('John Doe');
      