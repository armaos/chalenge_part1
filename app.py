import asyncio
from aiohttp import web
import asyncpg
from config import *
import re



async def init_db_pool(app):
    retries = 10
    delay = 2
    while retries:
        try:
            app['db_pool'] = await asyncpg.create_pool(**DATABASE_CONFIG)
            print("Database connection established")
            return
        except Exception as e:
            print(f"DB connection failed: {e}, retrying in {delay} seconds...")
            retries -= 1
            await asyncio.sleep(delay)
    raise Exception("Could not connect to the database.")

# Health check endpoint for the app
async def health_check(request):
    try:
        pool = request.app.get('db_pool')
        # Try a lightweight query to verify connectivity
        async with pool.acquire() as connection:
            await connection.fetchval("SELECT 1")
        return web.json_response({"status": "healthy"})
    except Exception as e:
        return web.json_response({"status": "unhealthy", "error": str(e)}, status=503)


async def init_db_pool(app):
    """Initialize the asyncpg connection pool on app startup."""
    app['db_pool'] = await asyncpg.create_pool(**DATABASE_CONFIG)

async def close_db_pool(app):
    """Close the asyncpg connection pool on app shutdown."""
    await app['db_pool'].close()

async def get_movies(request):
    """
    GET /movies – Fetch all movies from the database with optional sorting.
     Query parameters:
      limit (optional): Number of movies to return (default: 20)
      sort: <key>,<direction>   (Allowed keys: primarytitle, averagerating, startyear; direction: asc (default) or desc)
      after (optional): A cursor in the format "<value>,<tconst>" representing the last record from the previous page.
                         For the default sort, <value> is the startYear (an integer).
                         For a custom sort, <value> corresponds to the chosen column.
      genre: <genre_name>        (e.g., "Drama")
      rating_gt: <number>        (e.g., 7.0 to get movies with rating >= 7.0)
      rating_lt: <number>        (e.g., 8.5 to get movies with rating <= 8.5)
      Default ordering: tb.startYear ASC, tb.tconst ASC.
       If a custom sort is provided, ordering becomes:
      ORDER BY <allowed_keys[key]> <direction>, tb.tconst ASC.
    """
    pool = request.app['db_pool']

    # Define allowed sort keys and their corresponding SQL column expressions.
    allowed_keys = {
        'primarytitle': 'tb.primaryTitle',
        'averagerating': "COALESCE(tr.averageRating, -1)",
        'startyear': 'tb.startYear'
    }
   
    conditions = ["tb.titleType = 'movie'"]

    # Filter by genre if provided (using ILIKE for case-insensitive matching)
    genre_filter = request.query.get('genre')
    if genre_filter:
        try:
            conditions.append(f"tb.genres ILIKE '%{genre_filter}%'")
        except ValueError:
            return web.json_response({'error': 'Invalid genre_filter value'}, status=400)


    # Filter by rating greater than or equal if provided    
    rating_gt = request.query.get('rating_gt')
    if rating_gt:
        try:
            conditions.append(f"tr.averagerating >= {float(rating_gt)}")
        except ValueError:
            return web.json_response({'error': 'Invalid rating_gt value'}, status=400)



    # Filter by rating less than or equal if provided
    rating_lt = request.query.get('rating_lt')
    if rating_lt:
        try:
            conditions.append(f"tr.averagerating <= {float(rating_lt)}")
        except ValueError:
            return web.json_response({'error': 'Invalid rating_lt value'}, status=400)

   



    # Get limit (default to 20 if not provided)
    try:
        limit = int(request.query.get('limit', 20))
    except ValueError:
        return web.json_response({'error': 'Invalid limit value'}, status=400)
    # Get the cursor if provided



    # Get the sort query parameter if provided.
    sort_param = request.query.get('sort', '')
    order_clause = ""  # default ordering
    
    if sort_param:
        parts = sort_param.split(',')
        key = parts[0].lower()
        # Validate the sort key.
        if key in allowed_keys:
            # Determine the sort direction (default is ASC).
            direction = 'DESC' if (len(parts) > 1 and parts[1].lower() == 'desc') else 'ASC'
            order_clause = f"ORDER BY {allowed_keys[key]} {direction}, tb.tconst ASC"
        else:                    
            return web.json_response({
                'error': f'Invalid sort key. Allowed keys: {", ".join(allowed_keys.keys())}'
            }, status=400)
    else:
        key = 'startyear'
        order_clause = "ORDER BY tb.startYear ASC, tb.tconst ASC"


    # Build the pagination clause if the 'after' parameter is provided.
    after = request.query.get("after")
    if after:
        try:
            after_value_str, after_tconst = after.split(',')
            # Convert after_value to the correct type based on key.
            if key == 'startyear':
                after_value = int(after_value_str)
            elif key == 'averagerating':
                if after_value_str.lower() in ('null', 'none', ''):
                    after_value = -1
                else:
                    after_value = float(after_value_str)
            else:
                after_value = after_value_str  # for primarytitle, treat as string
        except Exception:
            return web.json_response({
                'error': 'Invalid "after" parameter format. Use "<value>,<tconst>"'
            }, status=400)
        
        # For keyset pagination, we need to filter rows after the given cursor.
        # We compare the primary sort column first, then tconst.
        pagination_clause = f"(({allowed_keys[key]} > $1) OR ({allowed_keys[key]} = $1 AND tb.tconst > $2))"
        conditions.append(pagination_clause)

        where_clause = "WHERE " + " AND ".join(conditions)
        query = f"""
            SELECT tb.tconst, tb.primaryTitle, tb.genres, tb.startYear, tb.runtimeMinutes, tr.averageRating
            FROM title_basics tb
            LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
            {where_clause}
            {order_clause}
            LIMIT $3
        """
        params = (after_value, after_tconst, limit)
    else:
        where_clause = "WHERE " + " AND ".join(conditions)
        # No pagination filter provided; use just the ORDER BY and LIMIT.
        query = f"""
            SELECT tb.tconst, tb.primaryTitle, tb.genres, tb.startYear, tb.runtimeMinutes, tr.averageRating
            FROM title_basics tb
            LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
            {where_clause}
            {order_clause}
            LIMIT $1
        """
        params = (limit,)
    
    
    
    print(query)
    async with pool.acquire() as connection:
        rows = await connection.fetch(query, *params)
    

    movies = []
    for row in rows:
        movies.append({
            'id': row['tconst'],
            'title': row['primarytitle'],
            'genre': row['genres'],
            'year': row['startyear'],
            'rating': row['averagerating'],
            'runtime': row['runtimeminutes'],
            'imdb_link': f'https://www.imdb.com/title/{row["tconst"]}/'
        })
    
    # Determine the next cursor from the last record in the result set.
    next_cursor = None
    if rows:
        last = rows[-1]
        sort_value = last[key.lower()]
        next_cursor = f"{sort_value},{last['tconst']}"
    
    return web.json_response({
        'movies': movies,
        'next_cursor': next_cursor
    })

    





async def get_movie(request):
    """GET /movies/{id} – Fetch a single movie by its id (tconst)."""
    movie_id = request.match_info.get('id')
    pool = request.app['db_pool']
    async with pool.acquire() as connection:
        row = await connection.fetchrow('''
            SELECT tb.tconst, tb.primarytitle, tb.genres, tb.startyear, tb.runtimeminutes, tr.averagerating
            FROM title_basics tb
            LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst
            WHERE tb.tconst = $1 AND tb.titletype = 'movie'
        ''', movie_id)
        if row:
            movie = {
                'id': row['tconst'],
                'title': row['primarytitle'],
                'genre': row['genres'],
                'year': row['startyear'],
                'rating': row['averagerating'],
                'runtime': row['runtimeminutes'],
                'imdb_link': f'https://www.imdb.com/title/{row["tconst"]}/'
            }
            return web.json_response(movie)
        else:
            return web.json_response({'error': 'Movie not found'}, status=404)

async def create_movie(request):
    """POST /movies – Create a new movie.
    
    Expected JSON payload:
    {
      "tconst": "tt1234567",
      "title": "Example Movie",
      "genre": "Action,Comedy",
      "year": 2020,
      "rating": 7.5,
      "runtime": 120
    }
    """
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'Invalid JSON payload'}, status=400)
        
    required_fields = ['tconst', 'title', 'genre', 'year', 'rating', 'runtime']
    if not all(field in data for field in required_fields):
        return web.json_response({'error': 'Missing required fields'}, status=400)
    
    tconst = data['tconst'].strip()
    title = data['title'].strip()
    genre = data['genre'].strip()
    year = data['year']
    rating = data['rating']
    runtime = data['runtime']
    # Security checks
    try:
        year = int(data['year'])
    except ValueError:
        return web.json_response({'error': 'Year must be an integer'}, status=400)
    
    try:
        rating = float(data['rating'])
    except ValueError:
        return web.json_response({'error': 'Rating must be a number'}, status=400)

    try:
        runtime = int(data['runtime'])
    except ValueError:
        return web.json_response({'error': 'Runtime must be an integer'}, status=400)
    
    # Additional validations:
    if not (1800 <= year <= 2100):
        return web.json_response({'error': 'Year must be between 1800 and 2100'}, status=400)
    
    if not (0.0 <= rating <= 10.0):
        return web.json_response({'error': 'Rating must be between 0 and 10'}, status=400)
    
    if runtime <= 0:
        return web.json_response({'error': 'Runtime must be a positive integer'}, status=400)
        
    # Check that tconst starts with "tt" and then one or more digits
    if not re.match(r'^tt\d+$', tconst):
        return web.json_response({'error': 'tconst must start with "tt" followed by digits'}, status=400)
    
    pool = request.app['db_pool']
    async with pool.acquire() as connection:
        try:
            async with connection.transaction():
                # Insert into title_basics with default values for titleType, originalTitle, isAdult, and endYear.
                await connection.execute('''
                    INSERT INTO title_basics(tconst, titletype, primarytitle, originaltitle, isadult, startyear, endyear, runtimeminutes, genres)
                    VALUES($1, 'movie', $2, $2, FALSE, $3, NULL, $4, $5)
                ''', tconst, title, year, runtime, genre)
                
                # Insert into title_ratings with default numVotes.
                await connection.execute('''
                    INSERT INTO title_ratings(tconst, averageRating, numVotes)
                    VALUES($1, $2, 0)
                ''', tconst, rating)
        except asyncpg.UniqueViolationError:
            return web.json_response({'error': 'Movie with that ID already exists'}, status=409)
    
    movie = {
        'id': tconst,
        'title': title,
        'genre': genre,
        'year': year,
        'rating': rating,
        'runtime': runtime,
        'imdb_link': f'https://www.imdb.com/title/{tconst}/'
    }
    return web.json_response(movie, status=201)

# Set up the aiohttp app and routes
app = web.Application()
app.router.add_get('/health', health_check)
app.router.add_get('/movies', get_movies)
app.router.add_get('/movies/{id}', get_movie)
app.router.add_post('/movies', create_movie)

# Register startup and cleanup signals for DB pool management.
app.on_startup.append(init_db_pool)
app.on_cleanup.append(close_db_pool)

if __name__ == '__main__':
    web.run_app(app, host='0.0.0.0', port=8080)