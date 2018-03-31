from flask import Flask
from flask import request
from flask import jsonify
from google.cloud import bigquery

app = Flask(__name__)

def query_result_parse(results):
    result_arr = []

    for row in results:
       result_arr.append(
           {
               'id': row.id,
               'tags': row.tags,
               'score': row.score,
               'title': row.title,
               'body': row.body
           }
       )
    
    return result_arr


def make_query(tag_list):

    main_query = """
    SELECT
        q.id,
        q.tags,
        q.score,
        q.title,
        a.body
    FROM
        `bigquery-public-data.stackoverflow.posts_questions` q
    ,
        `bigquery-public-data.stackoverflow.posts_answers` a
    WHERE
        q.id=a.parent_id
    AND
    """

    if len(tag_list) == 4:
        main_query += """
        q.tags LIKE '%{}%' and q.tags LIKE '%{}%' and a.body LIKE '%{}%' and a.body LIKE '%{}%'
        """.format(tag_list[0], tag_list[1], tag_list[2], tag_list[3])
    else:
        main_query += """
        q.tags LIKE '%{}%' and a.body LIKE '%{}%'
        """.format(tag_list[0], tag_list[1])
    
    main_query += """
    ORDER BY
        a.score DESC
    LIMIT
        5
    """

    return main_query


def query_process(request):
    tag_list = dict(request.args)['tag']
    client = bigquery.Client()
    query_job = client.query(make_query(tag_list))
    results = query_job.result()  # Waits for job to complete.

    return jsonify(query_result_parse(results))

@app.route('/setting')
def tag():
    return query_process(request) 


@app.route('/realtime')
def real_time():
    return query_process(request) 


if __name__ == '__main__':
    app.run(host='0.0.0.0')
