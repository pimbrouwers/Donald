DROP TABLE IF EXISTS author;

CREATE TABLE author (    
    author_id   INTEGER   NOT NULL  PRIMARY KEY  AUTOINCREMENT  
  , full_name   TEXT      NOT NULL  UNIQUE
  , birth_date  NUMERIC   NULL);

INSERT INTO author (full_name) 
VALUES
    ('Pim Brouwers')
  , ('John Doe');
      
CREATE TABLE file (    
    file_id   INTEGER   NOT NULL  PRIMARY KEY  AUTOINCREMENT  
  , data      BLOB      NOT NULL);
