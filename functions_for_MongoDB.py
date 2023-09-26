from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv, find_dotenv
import os
import pprint

load_dotenv(find_dotenv())

uri = os.environ.get("MONGO_CONNECTION_STRING")

# Create a new client and connect to the server
client = MongoClient(uri)
    
printer = pprint.PrettyPrinter()


def insert_one_doc(db, collection, doc) -> None:
    inserted_id = client[db][collection].insert_one(doc).inserted_id
    printer.pprint(f'You seccessfully inserted one document, document ids: \n {inserted_id}')
    return None

def insert_many_docs(db, collection,docs) -> None:
    inserted_ids = client[db][collection].insert_many(docs).inserted_ids
    printer.pprint(f'You seccessfully inserted many documents, documents ids: \n {inserted_ids}')
    return None

def find_docs(db, collection, argument = '', value = '') -> None:
    if argument == '' or value == '':
        docs = client[db][collection].find()
    else:
        if argument == '_id':
            from bson.objectid import ObjectId
            value = ObjectId(value)
        docs = client[db][collection].find({argument: value})
    if docs.count() == 0:
        printer.pprint('No such documents in this collection.')
        return None
    for doc in docs:
        printer.pprint(doc)
    return None

def count_docs(db, collection, filter = {}) -> None:
    count = client[db][collection].count_documents(filter=filter)
    printer.pprint(f'Number of documents in {db} database and {collection} collection is: {count}')
    return None

def find_docs_in_range(db, collection, argument = '', minvalue = '', maxvalue = '') -> None:
    if argument == '' or minvalue == '' or maxvalue == '':
        printer.pprint('Please give an argument, minvalue and maxvalue.')
        return None
    else:
        if argument == '_id':
            from bson.objectid import ObjectId
            minvalue = ObjectId(minvalue)
            maxvalue = ObjectId(maxvalue)
            
        querry = {
                "$and": [
                    {argument: {"$gte": minvalue}},
                    {argument: {"$lte": maxvalue}}
                    ]
                }
        
        docs = client[db][collection].find(querry)
    if docs.count() == 0:
        printer.pprint('No such documents in this collection.')
        return None
    for doc in docs:
        printer.pprint(doc)
    return None

def update_doc_by_id(db, collection, id, new_values) -> None:
    from bson.objectid import ObjectId
    id = ObjectId(id)
    update = {
        "$set": new_values
    }
    client[db][collection].update_one({"_id": id}, update)
    printer.pprint('Object was updated successfully!')
    return None

def delete_doc_by_id(db, collection, id) -> None:
    from bson.objectid import ObjectId
    id = ObjectId(id)
    client[db][collection].delete_one({"_id": id})
    printer.pprint('Object was deleted successfully!')
    return None

def delete_all_docs(db, collection) -> None:
    client[db][collection].delete_many(filter={})
    printer.pprint('All object were deleted successfully!')
    return None