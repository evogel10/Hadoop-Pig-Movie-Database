u_data = LOAD '/pig/u.data' AS (userID:int, itemID:int, rating:int, timeStamp:long);
u_item = LOAD '/pig/u.item' using PigStorage('|') AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoReleaseDate:chararray, imbdUrl:chararray, unknown:int, action:int, adventure:int, animation:int, childrens:int, comedy:int, crime:int, documentary:int, drama:int, fantasy:int, filmNoir:int, horror:int, musical:int, myster:int, romance:int, sciFi:int, thriller:int, war:int, western:int);
raw_u_data = FOREACH u_data GENERATE $1, $2;
raw_u_item = FOREACH u_item GENERATE $0, $1, $2, $4;
ratings = GROUP raw_u_data BY itemID;
avg_ratings = FOREACH ratings GENERATE group AS itemID, AVG(raw_u_data.rating) as avg_rating;
movie_ratings = JOIN avg_ratings BY itemID RIGHT OUTER, raw_u_item BY movieID;
movie_ratings = JOIN raw_u_item BY movieID LEFT OUTER, avg_ratings BY itemID;
raw_movie_ratings = FOREACH movie_ratings GENERATE .. $3, $5 ..;
STORE raw_movie_ratings INTO '/pig/out';

/usr/local/hadoop/pig-0.17.0/scripts

ratings = GROUP uData BY itemID;

avg_rating = FOREACH ratings GENERATE group, AVG(rating);

myJoin = JOIN A BY itemID RIGHT OUTER, B BY movieID;

avg_ratings = FOREACH ratings GENERATE (raw_u_data.itemID, raw_u_data.rating), AVG(raw_u_data.rating);

myGroup = GROUP A BY rating

myFileer = 
DUMP join

movieID INT, movie_title STRING, release_date STRING, 
video_release_date STRING, imdb_url STRING, unknown INT, action INT, 
adventure INT, animation INT, childrens INT, comedy INT, crime INT, 
documentary INT, drama INT, fantasy INT, film_noir INT, horror INT, 
musical INT, mystery INT, romance INT, sci_fi INT, thriller INT, war INT, 
western INT)