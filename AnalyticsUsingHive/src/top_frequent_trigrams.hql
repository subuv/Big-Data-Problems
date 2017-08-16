CREATE TABLE books (line STRING) PARTITIONED BY (book_name STRING);

LOAD DATA local INPATH '/usr/local/JarFiles/books/A mid summer night dream.txt'
INTO TABLE books
PARTITION(book_name="A mid summer night dream.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/Hamlet.txt'
INTO TABLE books
PARTITION(book_name="Hamlet.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/King Richard III.txt'
INTO TABLE books
PARTITION(book_name="King Richard III.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/MacBeth.txt'
INTO TABLE books
PARTITION(book_name="MacBeth.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/Othello.txt'
INTO TABLE books
PARTITION(book_name="Othello.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/Romeo and Juliet.txt'
INTO TABLE books
PARTITION(book_name="Romeo and Juliet.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/The Merchant of Venice.txt'
INTO TABLE books
PARTITION(book_name="The Merchant of Venice.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/The tempest.txt'
INTO TABLE books
PARTITION(book_name="The tempest.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/The tragedy of Julius Casear.txt'
INTO TABLE books
PARTITION(book_name="The tragedy of Julius Casear.txt");

LOAD DATA local INPATH '/usr/local/JarFiles/books/The tragedy of King Lear.txt'
INTO TABLE books
PARTITION(book_name="The tragedy of King Lear.txt");

INSERT OVERWRITE LOCAL DIRECTORY '/usr/local/JarFiles/Hive2_Out/'
SELECT EXPLODE(NGRAMS(SENTENCES(LOWER(line)), 3, 10)) AS trigrams FROM books;
