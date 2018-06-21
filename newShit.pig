ratings = LOAD '/user/aviral/ml-100k/u.data'
AS (
    userID: int,
    movieID: int,
    rating: int,
    ratingTime: int
);

metadata = LOAD '/user/aviral/ml-100k/u.item'
USING PigStorage('|')
AS (
    movieID: int,
    movieTitle: chararray,
    releaseDate: chararray,
    videoRelease: chararray,
    imdbLink: chararray
);

nameLookup = FOREACH metadata
GENERATE
    movieID,
    movieTitle,
    ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) as releaseDate;

ratingsByMovie = GROUP ratings BY movieID;

avgRatingsByMovie = FOREACH ratingsByMovie
GENERATE
    group AS movieID,
    AVG(ratings.rating) as avgRating;

moviesWithData = JOIN avgRatingsByMovie BY movieID, nameLookup BY movieID;

orderedMoviesWithData = ORDER moviesWithData BY avgRatingsByMovie::avgRating;

ltdData = LIMIT orderedMoviesWithData 10;

DUMP ltdData;
