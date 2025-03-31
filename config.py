import os
# DATABASE_CONFIG = {
#     'user': 'alexandrosarmaos',
#     'database': 'imdb_db',
#     'host': 'localhost'
# }

DATABASE_CONFIG = {
    'user': os.getenv('DATABASE_USER', 'imdb_user'),
    'password': os.getenv('DATABASE_PASSWORD', 'imdb_pass'),
    'database': os.getenv('DATABASE_NAME', 'imdb_db'),
    'host': os.getenv('DATABASE_HOST', 'db'),
    'port': os.getenv('DATABASE_PORT', 5432)
}