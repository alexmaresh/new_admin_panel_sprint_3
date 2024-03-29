from data_classes import MovieRaw, GenreRaw, PersonRaw


def _person_formatter(persons: dict) -> list:
    output = [{'uuid': k, 'full_name': v} for k, v in persons.items()]
    return output


def _genre_formatter(genres: dict) -> list:
    output = [{'uuid': k, 'name': v} for k, v in genres.items()]
    return output


def genres_transformer(genre_to_transform):
    data = GenreRaw(*genre_to_transform)
    doc = {
        'id': data.uuid,
        'name': data.name,
        'description': data.description,
    }
    return doc, data.uuid


def person_transformer(person_to_transform):
    data = PersonRaw(*person_to_transform)
    doc = {
        'id': data.uuid,
        'full_name': data.full_name
    }
    return doc, data.uuid


def transformer(movie_to_transform: list) -> tuple:
    director = set()
    genre = set()
    actor_names = set()
    writer_names = set()
    actors = dict()
    writers = dict()
    directors = dict()
    genres = dict()

    for row in movie_to_transform:

        data = MovieRaw(*row)
        genre.add(data.genre)
        genres[data.genre_id] = data.genre
        if data.role == 'director':
            director.add(data.person_name)
            directors[data.person_id] = data.person_id
        elif data.role == 'writer':
            writer_names.add(data.person_name)
            writers[data.person_id] = data.person_name
        elif data.role == 'actor':
            actor_names.add(data.person_name)
            actors[data.person_id] = data.person_name

    director = None if not director else ', '.join(director)
    genre = None if not genre else ', '.join(genre)
    actor_names = None if not actor_names else ', '.join(actor_names)
    writer_names = None if not writer_names else list(writer_names)

    doc = {
        'id': data.uuid,
        'imdb_rating': data.imdb_rating,
        'genre': _genre_formatter(genres),
        'title': data.title,
        'description': data.description,
        'director': director,
        'actors_names': actor_names,
        'writers_names': writer_names,
        'actors': _person_formatter(actors),
        'writers': _person_formatter(writers),
        'directors': _person_formatter(directors)

    }
    return doc, data.uuid


if __name__ == '__main__':
    pass
