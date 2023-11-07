import bson
from typing import Any
import datetime as dt
from pymongo import MongoClient


QUERY = [
    {
        '$match': {
            'dt': {
                '$gte': 'start_date',
                '$lte': 'end_date'
            }
        }
    },
    {
        '$group': {
            "_id": {
                "$dateTrunc": {
                    "date": '$dt',
                    "unit": 'month'
                }
            },
            "sum": {
                "$sum": '$value'
            }
        }
    },
    {
        '$sort': {
            '_id': 1
        }
    }
]


def my_algo(input_data: dict, collection: Any, query: list) -> dict:
    """Функция, которая принимает временные отрезки и способ агрегации от пользователя,
    вставляет их в запрос к MongoDB и возвращает полученные результаты."""

    start_date = dt.datetime.fromisoformat(input_data['dt_from'])
    end_date = dt.datetime.fromisoformat(input_data['dt_upto'])

    query[0]['$match']['dt']['$gte'] = start_date
    query[0]['$match']['dt']['$lte'] = end_date

    if input_data['group_type'] == 'month':
        query[1]['$group']['_id']['$dateTrunc']['unit'] = 'month'
        return get_result(collection=collection, query=query, list_date=[])

    elif input_data['group_type'] == 'day':
        query[1]['$group']['_id']['$dateTrunc']['unit'] = 'day'
        date_generated = [start_date + dt.timedelta(days=x) for x in range(0, (end_date - start_date).days + 1)]
        list_date = [date.strftime("%Y-%m-%dT%H:%M:%S") for date in date_generated]
        return get_result(collection=collection, query=query, list_date=list_date)

    elif input_data['group_type'] == 'hour':
        query[1]['$group']['_id']['$dateTrunc']['unit'] = 'hour'
        date_generated = [start_date + dt.timedelta(hours=x) for x in
                          range(0, ((end_date - start_date).days * 24) + 1)]
        list_date = [date.strftime("%Y-%m-%dT%H:%M:%S") for date in date_generated]
        return get_result(collection=collection, query=query, list_date=list_date)


def get_result(collection, query, list_date):
    """ Функция, которая получает готовый запрос к базе данных, выполняет его и возвращает результат запроса. """
    answer = {"dataset": [], "labels": []}
    result = list(collection.aggregate(query))

    for doc in result:
        """Вставляем в ответ пользователю полученные данные из базы."""

        answer['dataset'].append(doc['sum'])
        answer['labels'].append(dt.datetime.isoformat(doc['_id']))

    for index, value in enumerate(list_date):
        """Сверяем все данные (месяц,день,час), на всем временном отрезке с теми, которые имеются в базе. Если записи 
        с такой датой нет, вставляем в ответ дату и значение равное 0."""

        if value not in answer['labels']:
            answer['dataset'].insert(list_date.index(value), 0)
            answer['labels'].insert(list_date.index(value), value)
    return answer


def check_collection(collection_name):
    """Подключаемся к базе и коллекции, если такой базы и коллекции нет -- создаем."""

    client = MongoClient('mongodb://localhost:27017')
    db = client['db']
    if collection_name not in db.list_collection_names():
        start_collection = db['mycollection']
        with open('sample_collection.bson', 'rb') as f:
            data_for_add = bson.decode_all(f.read())
            start_collection.insert_many(data_for_add)
        return start_collection
    else:
        start_collection = db['mycollection']
        return start_collection


if __name__ == '__main__':
    """main функция, вызываемая ботом"""
    my_algo(input_data={}, collection=Any, query=[])
