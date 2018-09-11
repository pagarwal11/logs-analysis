#!/usr/bin/env python3

# "Database code" for the Logs.

import psycopg2

DBNAME = 'news'

# runs the query passed in parameter and returns the result


def run_query(query):
    try:
        db = psycopg2.connect("dbname=news")
    except psycopg2.Error as e:
        print("Unable to connect!")
        print(e.pgerror)
        print(e.diag.message_detail)
        sys.exit(1)
    else:
        c = db.cursor()
        c.execute(query)
        result = c.fetchall()
        db.close()
        return result

# query to find top three articles by popularity


def articles_by_popularty():
    query = """select title,count(*) as no_of_views from articles,log where
    log.path LIKE concat(\'%\',articles.slug)
    group by articles.title order by no_of_views desc limit 3"""
    result = run_query(query)
    print('The top three articles sorted by popularity are: ')
    for title, no_of_views in result:
        print('{} with {} views'.format(title, no_of_views))
    pass

# query to find top three authors by popularity


def authors_by_popularity():
    query = """select authors.name as name,sum(no_of_views) as total_author_views from
    (select author as distinct_author,count(*) as no_of_views
    from articles,log where log.path LIKE concat(\'%\',articles.slug)
    group by articles.title,
    articles.author) as no_of_views,authors where
    authors.id = distinct_author group by authors.name order by
    total_author_views desc"""
    result = run_query(query)
    print('The top authors sorted by popularity are: ')
    for name, total_author_views in result:
        print('{} with {} views'.format(name, total_author_views))
    pass

# query to find days on which more than 1% of total requests led to errors


def error_log():
    query = """select err_date,(err * 100.0 / total) as error_requests from
    (select date(time) as err_date,count(*) as err from log where
    log.status = \'404 NOT FOUND\' group by date(time)) as err_requests,
    (select date(time) as comparison_date,count(*) as total from log
    group by date(time)) as total_requests
    where err_date = comparison_date and err > 0.01 * total"""
    result = run_query(query)
    print('The dates with more than 1% error requests are: ')
    for err_date, error_requests in result:
        print('{} with {}% error requests'.format(err_date, error_requests))
    pass


if __name__ == "__main__":
    articles_by_popularty()
    authors_by_popularity()
    error_log()
