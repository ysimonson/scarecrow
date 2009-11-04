import hashlib
import datetime
from google.appengine.ext import db

#Try to import cPickle first because it's faster
try:
    import cPickle as pickle
except:
    import pickle
    
class Entity(db.Model):
    id = db.ByteStringProperty()
    updated = db.DateTimeProperty()
    body = db.BlobProperty()
    
    def __init__(self, key_name, *args, **kwargs):
        db.Model.__init__(self, key_name=key_name, *args, **kwargs)
    
class IndexedItem(db.Model):
    name = db.StringProperty()
    entity_id = db.ReferenceProperty()
    
    def __init__(self, name, *args, **kwargs):
        db.Model.__init__(self, *args, **kwargs)
        self.name = name

class Model(object):
    def __init__(self, *indexes):
        self.indexes = {}
        
        for index in indexes:
            self.indexes[index.name] = index
            index.model = self
        
    def install(self, drop=False):
        """
        Installs the database schema. Drops any currently existing tables if
        drop = True. This is just a stub method because the GAE datastore
        doesn't need to install a schema. Yay!
        """
        pass
        
    def __contains__(self, obj_id):
        query = db.Query(Entity, keys_only=True)
        return query.filter('__key__ =', obj_id).count(1) > 0
    
    def __iter__(self):
        for entity in Entity.all():
            yield entity
    
    def __len__(self):
        query = db.Query(Entity, keys_only=True)
        
        #Note that due to limitations in the GAE datastore, this will return a
        #maximum of 1000.
        return query.count()
        
    def __getitem__(self, obj_id):
        entity = db.get(obj_id)
        return pickle.loads(entity.body) if entity is not None else None
    
    def __setitem__(self, obj_id, obj):
        #Resets any data for the indexes
        for index in self.indexes:
            query = db.Query(index.__class__, keys_only=True)
            db.delete(query.filter("entity_id =", obj_id))
            
        #Insert the new object
        entity = Entity(obj_id)
        entity.body = pickle.dumps(obj)
        entity.updated = datetime.datetime.now()
        entity.put()
        
        #Run a map operation for the object for each index
        for index in self.indexes:
            self.indexes[index].map(db, obj_id, obj)
    
    def __delitem__(self, obj_id):
        query = db.Query(Entity, keys_only=True)
        db.delete(query.filter('__key__ =', obj_id))
    
    def get_last_update(self, obj_id):
        """
        Gets the time in which the object identified by obj_id was last updated
        """
        query = db.GqlQuery("SELECT updated FROM Entity WHERE __key__ = :1", obj_id)
        return query.get()
        
    def index(self, name, query, *args, **kwargs):
        """
        Executes an index query, contained in the index identified by name,
        whose name is identified by query. Passes along the args and kwargs.
        """
        index = self.indexes[name]
        func = getattr(index, query)
        return func(*args, **kwargs)
        
class AttributeIndex(object):
    def __init__(self, name, property, datatype):
        datatype = python_to_gae_type(datatype)
        
        class TypedAttributeIndex(IndexedItem):
            attr = datatype()
            
            def __init__(self, *args, **kwargs):
                IndexedItem.__init__(self, name, *args, **kwargs)
            
        self.schema = TypedAttributeIndex
        self.name = name
        self.property = property
        
    def install(self, db):
        pass
    
    def map(self, db, obj_id, obj):
        if hasattr(obj, self.property):
            value = getattr(obj, self.property)
            index = self.schema(key_name=obj_id)
            index.attr = value
            index.put()
    
    def get(self, db, value):
        """Gets a list of objects by attribute value"""
        query = db.GqlQuery('SELECT entity_id FROM TypedAttributeIndex WHERE name=:1 AND attr=:2',
                            self.name, self.value)
        
        for entity_id in query:
            entity = db.get(entity_id)
            if entity is not None: yield pickle.loads(entity.body)
                
    def count(self, db, value):
        """Gets how many objects have a specified attribute value"""
        query = db.Query(self.schema, keys_only=True)
        
        #Note that due to limitations in the GAE datastore, this will return a
        #maximum of 1000.
        return query.filter('name =', self.name).filter('attr =', value).count()
    
    def get_ids(self, db, value):
        """Gets the IDs of the objects with a specified attribute value"""
        results = db.GqlQuery("SELECT entity_id FROM TypedAttributeIndex WHERE name=:1 AND attr=:2",
                           self.name, value)
        return results
    
    def get_range(self, db, start, end):
        """Gets a list of objects by a range of values for an attribute"""
        rquery = db.GqlQuery('SELECT entity_id FROM TypedAttributeIndex WHERE name=:1 AND attr>:2 AND attr<:2',
                            self.name, self.value)
        
        for entity_id in query:
            entity = db.get(entity_id)
            if entity is not None: yield pickle.loads(entity.body)
                    
    def get_range_ids(self, db, start, end):
        """Gets the IDs of the objects with a specified range of attribute values"""
        results = db.GqlQuery("SELECT entity_id FROM TypedAttributeIndex WHERE name=:1 AND attr=:2",
                           self.name, value)
        return results

datatype_dict = {
    int: db.IntegerProperty,
    long: db.IntegerProperty,
    float: db.FloatProperty,
    
    str: db.StringProperty,
    unicode: db.StringProperty,
    
    datetime.datetime: db.DateTimeProperty,
    datetime.date: db.DateProperty,
    datetime.time: db.TimeProperty,
}

def python_to_gae_type(datatype):
    try:
        return datatype_dict[datatype]
    except:
        raise TypeError()