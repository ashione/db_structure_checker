from sqlalchemy import create_engine
from sqlalchemy import inspect, create_engine
from termcolor import colored


def print_wrap(func):
    def __decorator(self,compared):
        print '*' * 30 
        print '%s bettween (%s,%s)\n' %(func.func_name,self.db_name,compared.db_name)
        ret = func(self,compared)
        print '*' *30
        return ret
    return __decorator


class DBConfig(object):

    def __init__(self,db_user,db_password,db_host,db_name,db_port=3306,db_engine='mysql'):
        self.db_user = db_user;
        self.db_password = db_password
        self.db_host = db_host
        self.db_name = db_name
        engine = create_engine('{db_engine}://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'.format(
        db_user=db_user,db_password=db_password,db_host=db_host,
        db_name=db_name,db_port=db_port,db_engine=db_engine))
        self.insp = inspect(engine)

    def table_name_set(self):
        return set(self.insp.get_table_names())

    def table_column(self,table_name):
        return self.insp.get_columns(table_name)
    
    def table_indexes(self,table_name):
        return self.insp.get_indexes(table_name)

    def table_foreign_keys(self,table_name):
        return self.insp.get_foreign_keys(table_name)

    def compare_table_name(self,compared):
        if set(self.table_name_set()) & set(compared.table_name_set()):
            print colored('table set are same','green')


    @staticmethod
    def compare_meta(origin_meta,variant_meta,table_name):
        if len(origin_meta) != len(variant_meta):
            print colored('columns of this table (%s) is not eq to variant' 
                    %(table_name),'red')
            print colored('\torigin structure: %s' %(origin_meta),'red')
            print colored('\tvariant structure: %s' %(variant_meta),'red')

        v_table_dict = { x['name'] : x for x in variant_meta}
        o_table_dict = { x['name'] : x for x in origin_meta}

        if len(v_table_dict.keys()) == len(o_table_dict.keys()) and \
           len(v_table_dict.keys())  == 0:
            print 'empty meta in table %s' %(table_name)
            return True

        if not (set(v_table_dict.keys()) == set(o_table_dict.keys())):
            set_v = set(v_table_dict.keys())
            set_o = set(o_table_dict.keys())
            print colored('columes not eq in table %s' %(table_name),'red')
            print colored('\tcommon set : %s' %(set_v & set_o),'red')
            print colored('\torigin diff variant %s' %(set_o - set_v),'red')
            print colored('\tvariant diff origin %s' %(set_v - set_o),'red')
            return False

        flag = True

        for o_column in o_table_dict.keys():
            for o_property in o_table_dict[o_column]:

                if not v_table_dict[o_column].has_key(o_property):
                    flag = False
                    print colored('variant table (%s) has no property %s'
                            %(table_name,o_property),
                            'red')
                    continue

                if v_table_dict[o_column][o_property] != o_table_dict[o_column][o_property] \
                   and ( o_property == 'type' and \
                   str(v_table_dict[o_column][o_property]) != \
                   str(o_table_dict[o_column][o_property])):
                    #import ipdb
                    #ipdb.set_trace()

                    flag = False
                    print colored('table %s column %s, property %s not eq [%s -> %s]' \
                            %(table_name,o_column,o_property,
                              v_table_dict[o_column][o_property],
                              o_table_dict[o_column][o_property]),
                            'red')
        if flag:
            print colored('table %s columns are same' %(table_name),'green')
        return flag


    @print_wrap
    def compare_table_column(self,compared):
        table_column_flag = True
        for table_name in self.table_name_set():
            origin_table = self.table_column(table_name) 
            variant_table = compared.table_column(table_name)
            table_column_flag = table_column_flag & self.compare_meta(origin_table,variant_table,table_name)
        return table_column_flag


    @print_wrap
    def compare_table_indexes(self,compared):
        table_indexes_flag = True
        for table_name in self.table_name_set():
            origin_table_indexes = self.table_indexes(table_name) 
            variant_table_indexes = compared.table_indexes(table_name)
            table_indexes_flag = table_indexes_flag & self.compare_meta(origin_table_indexes,variant_table_indexes,table_name)
        return table_indexes_flag

    @print_wrap
    def compare_table_fk(self,compared):
        table_fk_flag = True
        for table_name in self.table_name_set():
            origin_table_fk = self.table_foreign_keys(table_name) 
            variant_table_fk = compared.table_foreign_keys(table_name)
            table_fk_flag = table_fk_flag & self.compare_meta(origin_table_fk,variant_table_fk,table_name)
        return table_fk_flag


    def __str__(self):
        tables = self.table_name_set()
        return str({
            'Table_names' : tables,
            'Table_columns' : [self.table_column(table_name) for table_name in
                tables],
            'Table_indexes' : [self.table_indexes(table_name) for table_name in
                tables],
            'Table_foreign_keys' : [self.table_foreign_keys(table_name) for
                table_name in tables]})

    def __eq__(self,other):
        if self.compare_table_name(other) and \
           self.compare_table_column(other) and \
           self.compare_table_indexes(other) and \
           self.compare_table_fk(other):
               return True
        return False
           

if __name__  == '__main__':

    nova_origin = DBConfig('root','xxx','xxx','nova')
    nova_variant = DBConfig('root','xxx','xxx','nova_variant')

    nova_origin == nova_variant

    print nova_origin.compare_table_name(nova_variant)
    print nova_origin.compare_table_column(nova_variant)
    print nova_origin.compare_table_indexes(nova_variant)
    print nova_origin.compare_table_fk(nova_variant)
