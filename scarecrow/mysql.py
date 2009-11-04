from tornado import database
import hashlib
import datetime

#Try to import cPickle first because it's faster
try:
    import cPickle as pickle
except:
    import pickle

class Model(object):
    def __init__(self, host, dbname, username, password, *indexes):
        self.host = host
        self.dbname = dbname
        self.username = username
        self.password = password
        
        self.indexes = {}
        for index in indexes:
            self.indexes[index.name] = index
            index.model = self
        
    def _connect(self):
        """Acquires a connection to the database"""
        return database.Connection(self.host, self.dbname, self.username, self.password)
        
    def install(self, drop=False):
        """
        Installs the database schema. Drops any currently existing tables if
        drop = True
        """
        db = self._connect()
        
        if drop: db.execute("DROP TABLE IF EXISTS entities")
        db.execute("""CREATE TABLE entities (
                        added_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        id BINARY(16) NOT NULL,
                        updated TIMESTAMP NOT NULL,
                        body MEDIUMBLOB,
                        UNIQUE KEY (id),
                        KEY (updated)
                      ) ENGINE=InnoDB
                   """)
        
        #Install the schema for each of the indexes
        for index in self.indexes:
            if drop: db.execute("DROP TABLE IF EXISTS %s" % index)
            self.indexes[index].install(db)
        
        db.close()
        
    def __contains__(self, obj_id):
        db = self._connect()
        result = db.get("SELECT COUNT(id) AS count FROM entities WHERE id=%s LIMIT 1", obj_id)
        count = result.count
        
        db.close()
        return count > 0
    
    def __iter__(self):
        db = self._connect()
        results = db.query("SELECT id FROM entities")
        
        for row in results:
            yield row.id
            
        db.close()
    
    def __len__(self):
        db = self._connect()
        result = db.get("SELECT COUNT(id) AS count FROM entities")
        count = result.count
        
        db.close()
        return count
        
    def __getitem__(self, obj_id):
        db = self._connect()
        
        result = db.get("SELECT body FROM entities WHERE id=%s LIMIT 1", obj_id)
        if result == None: return None
        obj = pickle.loads(result.body)
        
        db.close()
        return obj
    
    def __setitem__(self, obj_id, obj):
        db = self._connect()
        obj_body = pickle.dumps(obj)
        
        #Resets any data for the indexes
        for index in self.indexes:
            db.execute("DELETE FROM " + index + " WHERE entity_id=%s", obj_id)
        
        #Insert the new object
        db = self._connect()
        db.execute("""INSERT INTO entities (id, updated, body) VALUES (%s, NOW(), %s)
                      ON DUPLICATE KEY UPDATE updated=NOW(), body=%s""", obj_id, obj_body, obj_body)
        
        #Run a map operation for the object for each index
        for index in self.indexes:
            self.indexes[index].map(db, obj_id, obj)
        
        db.close()
    
    def __delitem__(self, obj_id):
        db = self._connect()
        
        #Delete any index data for the object
        for index in self.indexes:
            db.execute("DELETE FROM " + index + " WHERE entity_id=%s", obj_id)
        
        #Delete the object
        db.execute("DELETE FROM entities WHERE id=%s", obj_id)
        db.close()
    
    def get_last_update(self, obj_id):
        """
        Gets the time in which the object identified by obj_id was last updated
        """
        db = self._connect()
        
        result = db.get("SELECT updated FROM entities WHERE id=%s LIMIT 1", obj_id)
        if result == None:
            updated = None
        else:
            updated = result.updated
            
        db.close()
        return updated
        
    def index(self, name, query, *args, **kwargs):
        """
        Executes an index query, contained in the index identified by name,
        whose name is identified by query. Passes along the args and kwargs.
        """
        index = self.indexes[name]
        func = getattr(index, query)
        
        #Run the function with a newly acquired db connection
        db = self._connect()
        result = func(db, *args, **kwargs)
        
        db.close()
        return result

class AttributeIndex(object):
    def __init__(self, name, property, datatype):
        self.name = name
        self.property = property
        self.datatype = python_to_mysql_type(datatype)
        
    def install(self, db):
        db.execute("""CREATE TABLE %s (
                        %s %s NOT NULL,
                        entity_id BINARY(16) NOT NULL,
                        PRIMARY KEY (entity_id)
                      ) ENGINE=InnoDB
                   """ % (self.name, self.property, self.datatype))
    
    def map(self, db, obj_id, obj):
        if hasattr(obj, self.property):
            value = getattr(obj, self.property)
            db.execute("INSERT INTO " + self.name + " VALUES (%s, %s)", value, obj_id)
    
    def get(self, db, value):
        """Gets a list of objects by attribute value"""
        results = db.query("SELECT body FROM entities JOIN %s ON entities.id=%s.entity_id WHERE %s=%s"
                        % (self.name, self.name, self.property, '%s'), value)
        
        if results == None: return
        
        for row in results:
            yield pickle.loads(row.body)
                
    def count(self, db, value):
        """Gets how many objects have a specified attribute value"""
        results = db.get("SELECT COUNT(entity_id) AS count FROM %s WHERE %s=%s"
                        % (self.name, self.property, '%s'), value)
        
        if results == None: return 0
        return results.count
    
    def get_ids(self, db, value):
        """Gets the IDs of the objects with a specified attribute value"""
        results = db.query("SELECT entity_id FROM %s WHERE %s=%s" % (self.name, self.property, '%s'), value)
        
        if results == None: return
        for row in results: yield row.entity_id
    
    def get_range(self, db, start, end):
        """Gets a list of objects by a range of values for an attribute"""
        results = db.query("SELECT body FROM entities JOIN %s ON entities.id=%s.entity_id WHERE %s>=%s AND %s<=%s"
                           % (self.name, self.name, self.property, '%s', self.property, '%s'), start, end)
        
        if results == None: return
        
        for row in results:
            yield pickle.loads(row.body)
                    
    def get_range_ids(self, db, start, end):
        """Gets the IDs of the objects with a specified range of attribute values"""
        results = db.query("SELECT entity_id FROM %s WHERE %s>=%s AND %s<=%s"
                           % (self.name, self.property, '%s', self.property, '%s'), start, end)
        
        if results == None: return
        for row in results: yield row.entity_id

datatype_dict = {
    int: 'INT',
    long: 'BIGINT',
    float: 'FLOAT',
    
    str: 'TINYTEXT',
    unicode: 'TINYTEXT',
    
    datetime.datetime: 'DATETIME',
    datetime.date: 'DATE',
    datetime.time: 'TIME',
}

def python_to_mysql_type(datatype):
    try:
        return datatype_dict[datatype]
    except:
        raise TypeError()