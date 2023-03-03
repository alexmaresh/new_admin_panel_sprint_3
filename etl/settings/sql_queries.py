sql_get_top_time_person = '''SELECT modified from content.person ORDER BY modified DESC LIMIT 2;'''

sql_get_top_time_genre = '''SELECT modified from content.genre ORDER BY modified DESC LIMIT 2;'''

sql_push_persons = '''SELECT fw.id, fw.modified
                        FROM content.film_work fw
                    LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                    WHERE pfw.person_id IN %s
                    ORDER BY fw.modified
                    LIMIT 1000;'''

sql_check_persons = '''SELECT id, modified FROM content.person WHERE person.modified > %s
           order by modified DESC'''

sql_push_genres = '''SELECT fw.id, fw.modified
                                FROM content.film_work fw
                            LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id
                            WHERE pfw.genre_id IN %s
                            ORDER BY fw.modified
                            LIMIT 1000;'''

sql_check_genres = '''SELECT id, modified FROM content.genre WHERE genre.modified > %s
            order by modified DESC'''

sql_get_new_ids = '''SELECT id, modified from content.film_work WHERE film_work.modified > %s ORDER BY modified;'''

sql_get_film = '''SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            pfw.role,
            p.id,
            p.full_name,
            g.name,
            g.id
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id = %s;
        '''
