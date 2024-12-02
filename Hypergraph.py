# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 15:48:43 2024

@author: Thomas Urech
@author:Amani Nasar Tahat
@author: Maurice HT Ling
"""

import shelve
import random
from faker import Faker

fake = Faker()


def table_keys():
    return {
        "customer": "userid",
        "cart_item": ("userid", "productid"),
        "supplier": "supplierid",
        "product": "productid",
        "order": "orderid",
        "order_content": ("orderid", "productid"),
        "payment": "orderid",
    }


def table_foreign_keys():
    return {
        "cart_item": ("customer", "product"),
        "product": "supplier",
        "order": "customer",
        "order_content": ("order", "product"),
        "payment": "order",
    }


def init_hypergraph():
    """
    Creates a blank shelve file with the proper table names
    If there was a shelve file named name before, it gets cleared
        @param name : the name of the shelve file to opem
        @type name : string
        @default name : "hypergraph"
        @param tables_and_keys : the tables (and their primary keys) to initialize the hypergraph with
        @type tables_and_keys : a dictionary of table names to primary key names
        @default tables_and_keys : TABLE_KEYS defined at the beginning
    """
    hypergraph = shelve.open("hypergraph")
    hypergraph.clear()
    tables_and_keys = table_keys()
    for table_name in tables_and_keys:
        hypergraph[table_name] = {}
    hypergraph.sync()
    hypergraph.close()


def load_hypergraph(hypergraph):
    """
    Loads in a dictionary from the provided shelve file dictionary
    Only works using the defaults established in TABLE_KEYS
        @param hypergraph : the complete database
        @type hypergraph : a shelve
        @return : a 3-level dictionary representing the hypergraph
    """
    customer = hypergraph["customer"]
    cart_item = hypergraph["cart_item"]
    supplier = hypergraph["supplier"]
    product = hypergraph["product"]
    order = hypergraph["order"]
    order_content = hypergraph["order_content"]
    payment = hypergraph["payment"]
    db = {
        "customer": customer,
        "cart_item": cart_item,
        "supplier": supplier,
        "product": product,
        "order": order,
        "order_content": order_content,
        "payment": payment,
    }
    return db


def commit_hypergraph(hypergraph, db):
    """
    Updates the hypergraph shelve file to match the contents of DB
        @param hypergraph : the hypergraph shelve file's dictionary
        @type hypergraph : a shelve
        @param db : the updated database
        @type db : a 3-level dictionary
    """
    tables_and_keys = table_keys()
    for t in tables_and_keys:
        hypergraph[t] = db[t]
    hypergraph.sync()


"""element generation
Each of these functions take values for every attribute of a table and convert them into an element fit for the table
Use get_primary_key to get the key where each element should be stored at
Use TABLE_KEYS to get the names of primary keys
"""


def generate_customer_val(userid, email, username, shipping_address, billing_address):
    customer = {
        "userid": userid,
        "email": email,
        "username": username,
        "shipping_address": shipping_address,
        "billing_address": billing_address,
    }
    return customer


def generate_supplier_val(supplierid, supplier_name, shipping_address, billing_address):
    supplier = {
        "supplierid": supplierid,
        "supplier_name": supplier_name,
        "shipping_address": shipping_address,
        "billing_address": billing_address,
    }
    return supplier


def generate_product_val(productid, product_name, stock, price, supplierid):
    product = {
        "productid": productid,
        "product_name": product_name,
        "stock": stock,
        "price": price,
        "supplierid": supplierid,
    }
    return product


def generate_order_val(orderid, order_status, userid):
    order = {"orderid": orderid, "order_status": order_status, "userid": userid}
    return order


def generate_order_content_val(orderid, productid, quantity, feedback):
    order_content = {
        "orderid": orderid,
        "productid": productid,
        "quantity": quantity,
        "feedback": feedback,
    }
    return order_content


def generate_cart_item_val(userid, productid, quantity):
    cart_item = {"userid": userid, "productid": productid, "quantity": quantity}
    return cart_item


def generate_payment_val(orderid, amount, payment_method):
    payment = {"orderid": orderid, "amount": amount, "payment_method": payment_method}
    return payment


def create_address(street, city, state, zipcode, country):
    address = {
        "street": street,
        "city": city,
        "state": state,
        "zipcode": zipcode,
        "country": country,
    }
    return address


def address_string(address):
    return f"{address['street']} {address['city']}, {address['state']} {address['zipcode']} {address['country']}"


def parse_address(address):
    comma_split = address.split(", ")
    pre_comma = comma_split[0]
    splitter = pre_comma.split(" ", 3)
    street = splitter[0] + " " + splitter[1] + " " + splitter[2]
    city = splitter[3]
    post_comma = comma_split[1]
    splitter = post_comma.split(" ", 2)
    state = splitter[0]
    zipcode = splitter[1]
    country = splitter[2]
    return create_address(street, city, state, zipcode, country)


def calc_cost(db, orderid, userid):
    order = db["order"]
    content = db["order_content"]
    product = db["product"]
    the_ord = order[orderid]
    if the_ord["userid"] != userid:
        raise Exception("This is not the user's order")
    oc = select(content, f"orderid={orderid}")
    pidlist = project("productid", oc)
    plist = select(product, f"productid${pidlist}")
    prices = project("productid,price", plist)
    quantities = project("productid,quantity", oc)
    pq = inner_join(prices, quantities, "productid")
    pq = rename(pq, "productid.price", "price")
    pq = project("price,quantity", pq)
    total = 0
    for p, q in pq.items():
        total += p * q
    return total


def detect_conditional(where):
    conditional = ""
    if len(where) > 0:
        if len(where.split("=")) > 1:
            conditional = "="
        elif len(where.split("<")) > 1:
            conditional = "<"
        elif len(where.split(">")) > 1:
            conditional = ">"
        elif len(where.split("$")) > 1:  # element of
            conditional = "$"
    return conditional


"""insert and update"""


def insert(db, tablename, primary_key, element):
    """ "
    Inserts element into db[tablename][primary_key]
    Check foriegn key consistency: insert will fail if foreign key reference is invalid
        @param db: the hypergraph in its entirety
        @type db: a 3-level dictionary
        @param tablename: which table is being inserted into
        @type tablename: a string which is a key in db
        @param primary_key: the key to insert the element at
        @type primery_key: a number or a tuple of two numbers, which is a key in db[tablename]
        @param element: the database element to be inserted
        @type element: a 1-level dictionary
        @return FOREIGN_KEY_ERROR if insert failed due to invalid foreign key
    """
    tables_and_keys = table_keys()
    # check foreign keys
    # list of the tables this table has foreign keys to
    try:
        foreign_keys = get_foreign_key_list(tablename)
        # for each table this table has foreign keys to
        for fk in foreign_keys:
            # find out the name of the foreign key (primary key shares name with foreign key)
            fk_name = tables_and_keys[fk]
            # find the value of the attribute with that name in element
            fk_val = element[fk_name]
            # if that value is not a primary key in the foreign table
            # then this statement will throw an error, voiding the insert
            db[fk][fk_val]
    except KeyError:
        print(
            f"invalid {tablename} insert: foreign key {fk_name}:{fk_val} not present in {fk} table"
        )
    else:
        # if foriegn keys are sound, then insert the element into the correct table
        db[tablename][primary_key] = element


def update(db, tablename, primary_key, attributes_values):
    """
    Updates the attributes and values given at db[tablename][primary_key]
    If the primary_key is updated, then delete the old element before inserting the new one
    Insertion will result in a foreign key error if the foreign key is invalid
    Deletion will cause a deletion cascade if primary_key is another element's foreign key
    Deletion will only occur if insertion was successful
        @param db: the hypergraph in its entirety
        @type db: a 3-level dictionary
        @param tablename: the table to be updated
        @type tablename: a string which is a key in db
        @param primary_key: the key to the element to be updated
        @type primary_key: a number or a tuple of two numbers, which is a key in db[tablename]
        @param attributes_values: the attributes to be updated and the value to update it to
        @type attributes_values: a dictionary whose keys are attributes of tablename
        @return FOREIGN_KEY_ERROR via insert if the updated element's foreign key is invalid
    """
    tables_and_keys = table_keys()
    element = db[tablename][primary_key]
    for a in attributes_values:
        element[a] = attributes_values[a]
    pkname = tables_and_keys[tablename]
    if type(pkname) is tuple:
        newpk = (element[pkname[0]], element[pkname[1]])
    else:
        newpk = element[pkname]
    try:
        insert(db, tablename, primary_key, element)
        if newpk != primary_key:
            print("deleting")
            delete(db, tablename, primary_key)
    except:
        KeyError


def delete(db, tablename, primary_key):
    """
    Pops db[tablename][primary_key], deleting the element
    If any other element in the database had primary_key as its foreign key, it get deleted too
    This cascade deletion is recusrive
        @param db: the hypergraph in its entirety
        @type db: a 3-level dictionary
        @param tablename: the table to delete an element from
        @type tablename: a string which is a key in db
        @param primary_key: the key to the element to be deleted
        @type primary_key: a number or a tuple of two numbers, which is a key in db[tablename]
    """
    tables_and_keys = table_keys()
    foreign_keys = table_foreign_keys()
    # cascading delete only in this DB
    # check every table which has a foriegn key
    for foreign_table in foreign_keys:
        # if the table being deleted from is a foriegn key for that table
        fks = get_foreign_key_list(foreign_table)
        if tablename in fks:
            # check every element in the foriegn table
            ft = db[foreign_table]
            deleters = list()
            for cascade in ft:
                item = ft[cascade]
                cascade_key = item[tables_and_keys[tablename]]
                # if that element's foriegn key is the deleted element's primary key
                # that means it must be deleted to avoid a reference error
                if cascade_key == primary_key:
                    # run a FULL DELETE to continue the cascade
                    deleters.append(cascade_key)
            for ck in deleters:
                db = delete(db, foreign_table, ck)
    # after all cascading has been completed
    db[tablename].pop(primary_key)
    return db


def get_primary_key(tablename, element):
    """
    Figures out the primary key of the given element according to its table
    Looks up the primary key name(s) in TABLE_KEYS[tablename]
    If it is one key, then returns element[key]
    If it is two keys,then returns (element[key1],element[key2])
    The hypergraphs stores multikeys as tuples
        @param tablename: the name of the table element is in
        @type tablename: a string which is a key in the hypergraph (and TABLE_KEYS)
        @param element: the element to get the primary key of
        @type element: a 1-level dictionary
    """
    tables_and_keys = table_keys()
    key_list = list(tables_and_keys[tablename])
    if len(key_list) == 1:
        return element[key_list[0]]
    elif len(key_list) == 2:
        return (element[key_list[0]], element[key_list[1]])
    return


def get_foreign_key_list(tablename):
    """
    Converts the foreign keys of a table into an iterable list
    If the table has no foreign keys, returns an empty list
        @param tablename: the name of the table to get the foreign keys of
        @type tablename: a string which is a key in the hypergraph (and TABLE_KEYS)
        @return foreign_keys: the names of the tables this table has foreign keys to.
        @type foreign_keys: an array of strings, size 0 if no foreign key
    """
    foreign_keys = []
    foreign_table_keys = table_foreign_keys()
    try:
        foreign_keys = foreign_table_keys[tablename]
        if len(foreign_keys) == 2:
            foreign_keys = list(foreign_keys)
        else:
            foreign_keys = [foreign_keys]
    except KeyError:
        pass
    return foreign_keys


def clean_address(address):
    addr = address
    addr["zipcode"] = address["zip"]
    del addr["zip"]
    return addr


"""THE FUNCTIONS BELOW ARE NOT ORIGINAL WORK
They were taken from Mapping Relational Operations onto Hypergraph Model
Implementing a hypergraph is not the purpose of this project
The purpose of this project is comparing access speeds between hypergraphand relational database models.
Since there are few publicly available tools for hypergraph databases, 
this code was taken from a research paper so we could implement a hypergraph to test
Just as we do not claim DynamoDB as our original work, neither do we claim the code below
I did make some edits to the research code
"""


def rename(dic, old_key, new_key):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    Rename operator.
    Taken from Mapping Relational Operations onto Hypergraph Model
        @param dic: table (tuple) represented as graph
        @type dic: 2-level dictionary
        @param old_key: original field (attribute) name
        @type: string
        @param new_key: new field (attribute) name to be renamed to
        @type new_key: string
        @return: field name renamed table represented as graph
    """
    for k in dic:
        for kk in dic[k]:
            if kk == old_key:
                dic[k][new_key] = dic[k][old_key]
                del dic[k][old_key]
                break
    return dic


def select(db, where=""):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    Select (restriction) operator.
    Taken from Mapping Relational Operations onto Hypergraph Model
    Additional code added to support <, >, and in
        @param db: table(tuple) represented as graph
        @type db: 2-level dictionary
        @param where: selection condition in the format of <field name>=<condition>
        @type where: formatted string
        @default where: empty string
        @return: selected (restricted) table represented as graph
    """
    table = []
    conditional = detect_conditional(where)
    if len(where) > 0:
        where = where.split(conditional, 1)  # to avoid oversplitting
        where[0] = where[0].strip()
        where[1] = where[1].strip()
    if conditional == "$":
        where[1] = where[1].replace("[", "")
        where[1] = where[1].replace("]", "")
        table = where[1].split(", ")
    ret = {}
    for k in db:
        try:
            field = ""
            satisfy = False
            if len(where) == 2:
                field = db[k][where[0]]
                if conditional == "=":
                    satisfy = str(field) == str(where[1])
                elif conditional == ">":
                    satisfy = field > int(where[1])
                elif conditional == "<":
                    satisfy = field < int(where[1])
                elif conditional == "$":
                    satisfy = str(field) in table
            if satisfy or len(where) == 0:
                ret[k] = db[k]
        except KeyError:
            pass
    return ret


def project(columns, db):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    Projection operator.
    Taken from Mapping Relational Operations onto Hypergraph Model
        @param columns: comma-delimited list of fields (attributes) to project
        @type columns: string
        @param db: table(tuple) represented as graph
        @type db: 2-level dictionary
        @return: projected table represented as graph
    """
    columns = [x.strip() for x in columns.split(",")]
    ret = {}
    for k in db:
        if columns[0] == "*":
            ret[k] = db[k]
        else:
            ret[k] = {}
            for kk in columns:
                try:
                    ret[k][kk] = db[k][kk]
                except KeyError:
                    pass
    return ret


def left_join(left, right, key=None):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    THIS FUNCTION IS UNUSED
    Left join operator.
    Taken from Mapping Relational Operations onto Hypergraph Model
    Modification made to preserve left's foreign key
        @param left: table (tuple) at the left side of the join
        @type left: dictionary
        @param right: table (tuple) at the right side of the join
        @type right: dictionary
        @param key: name of joining field (attribute)
        @type key: string
        @return: joined table represented as graph
    """
    if not key:
        return {}
    ret = {}
    for k in left:
        ret[k] = left[k]
        fk = left[k][key]
        if fk in right:
            ret[k][key] = right[fk]
        ret[k] = flatten(ret[k])
        left[k][key] = fk
    return ret


def inner_join(left, right, key=None):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    Inner join operator
    Taken from Mapping Relational Operations onto Hypergraph Model
    Modification made to preserve left's foreign key
        @param left: table (tuple) at the left side of the join
        @type left: dictionary
        @param right: table (tuple) at the right side of the join
        @type right: dictionary
        @param key: name of joining field (attribute)
        @type key: string
        @return: joined table represented as graph
    """
    if not key:
        return {}
    ret = {}
    for k in left:
        fk = left[k][key]
        if fk in right:
            if len(right[fk]) > 0:
                # ret[k] = copy.copy(left[k])
                ret[k] = left[k]
                ret[k][key] = right[fk]
                ret[k] = flatten(ret[k])
                left[k][key] = fk
    return ret


def right_join(left, right, key=None):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    THIS FUNCTION IS UNUSED
    Right join operator
    Taken from Mapping Relational Operations onto Hypergraph Model
    Modification made to remove depreciated dictionary behavior.
        @param left: table (tuple) at the left side of the join
        @type left: dictionary
        @param right: table (tuple) at the right side of the join
        @type right: dictionary
        @param key: name of joining field (attribute)
        @type key: string
        @return: joined table represented as graph
    """
    ret = inner_join(left, right, key)
    for k in right:
        found = 0
        empty = {}
        for kk in left:
            if left[kk][key] == k:
                found = 1
        if found == 0:
            left_keys = list(left.keys())
            left_row = left[left_keys[0]]
            for kk in left_row:
                if kk == key:
                    empty[kk] = right[k]
                else:
                    empty[kk] = ""
            ret[k] = empty
            ret[k] = flatten(ret[k])
    return ret


def outer_join(left, right, key=None):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    THIS FUNCTION IS UNUSED
    Outer join operator
    Taken from Mapping Relational Operations onto Hypergraph Model
        @param left: table (tuple) at the left side of the join
        @type left: dictionary
        @param right: table (tuple) at the right side of the join
        @type right: dictionary
        @param key: name of joining field (attribute)
        @type key: string
        @return: joined table represented as graph
    """
    ret = left_join(left, right, key)
    for k in right:
        found = 0
        empty = {}
        for kk in left:
            if left[kk][key] == k:
                found = 1
        if found == 0:
            left_keys = left.keys()
            left_row = left[left_keys[0]]
            for kk in left_row:
                if kk == key:
                    empty[kk] = right[k]
                else:
                    empty[kk] = ""
            ret[k] = empty
            ret[k] = flatten(ret[k])
    return ret


def cartesian(left, right, keys=None):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    THIS FUNCTION IS UNUSED
    Cartesian (cross) join operator
    Taken from Mapping Relational Operations onto Hypergraph Model
        @param left: table (tuple) at the left side of the join
        @type left: dictionary
        @param right: table (tuple) at the right side of the join
        @type right: dictionary
        @param key: name of joining field (attribute)
        @type key: string
        @return: joined table represented as graph
    """
    if not keys:
        return {}
    ret = {}
    for left_key in left:
        for right_key in right:
            new_key = left_key + "_" + right_key
            ret[new_key] = left[left_key]
            ret[new_key][keys] = right[right_key]
            ret[new_key] = flatten(ret[new_key])
    for k in right:
        found = 0
        # empty = {}
        for kk in left:
            if left[kk][keys] == k:
                found = 1
        if found == 0:
            for kk in left:
                key = k + "_" + kk
                ret[key] = left[kk]
                ret[key][keys] = right[k]
                ret[key] = flatten(ret[key])
    return ret


def natural_join(left, right):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    THIS FUNCTION IS NOT USED
    Natural join operator
    Taken from Mapping Relational Operations onto Hypergraph Model
        @param left: table (tuple) at the left side of the join
        @type left: dictionary
        @param right: table (tuple) at the right side of the join
        @type right: dictionary
        @return: joined table represented as graph
    """
    for k in left:
        keys1 = left[k].keys()
        for kk in right:
            keys2 = right[kk].keys()
            break
        break
    key = ""
    for k in keys1:
        for kk in keys2:
            if k == kk:
                key = k
                break
        if key != "":
            break
    if key == "":
        return {}
    ret = {}
    for k in left:
        if right.has_key(left[k][key]):
            if len(right[left[k][key]]) > 0:
                ret[k] = left[k]
                ret[k][key] = right[left[k][key]]
                ret[k] = flatten(ret[k])
    return ret


def flatten(d, prefix=None, sep="."):
    """THIS FUNCTION IS NOT ORIGINAL WORK
    Flattening of a 3-level dictionary structure after join operations into a
    2-level dictionary structure. Modified from Terry Jones, 18 February 2009
    post on comp.lang.python – flatten a dict.
    Taken from Mapping Relational Operations onto Hypergraph Model
    Additional code added to remove depreciated dict.iteritems function
    And to recombine addresses into individual graphs
        @param d: dictionary to flatten
        @type d: 2-level or 3-level dictionary of dictionary
        @param prefix: prefix in flattened keys, default=None
        @type prefix: string
        @param sep: separator for flattened keys, default='.'
        @type sep: string
        @return: 2-level flattened dictionary
    """
    result = {}
    if prefix is None:
        prefix = ""
    for k, v in d.items():
        if prefix:
            key = prefix + sep + k
        else:
            key = k
        if isinstance(v, dict):
            if v and not "street" in v:
                result.update(flatten(v, key))
            elif "street" in v:
                result[key] = v
            else:
                result[key] = None
        else:
            result[key] = v
    return result


"""END MAPPING RELATIONAL OPERATIONS ONTO HYPERGRAPH SECTION"""

"""Insert/delete"""


def add_item_to_cart(userid, productid, quantity):
    """
    Adds quantiy of a product to the user's cart
        @param userid :The id of the user adding this product to their cart
        @type userid : integer
        @param productid : The id of the product being added to the cart
        @type productid : integer
        @param quantity : How many of the item are being added to the cart
        @type quantity : integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    ci = generate_cart_item_val(userid, productid, quantity)
    insert(db, "cart_item", (userid, productid), ci)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def remove_item_from_cart(userid, productid):
    """
    Removes an product from a user's cart
        @param userid : The id of the user removing a product from their cart
        @type userid : integer
        @param productid : The id of the product being removed from the cart
        @type productid : integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    pk = (userid, productid)
    delete(db, "cart_item", pk)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def create_new_customer(cust_dict):
    """
    Creates a new customer. Returns the customer's userid.
        @param cust_dict a dictionary containing the customer information
        @type cust_dict: a dictionary with dictionaries for addresses
        @return : new customer's userid as integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    customer = db["customer"]
    pk = 1
    if len(customer) > 0:
        pk = max(customer.keys()) + 1
    username = cust_dict["username"]
    email = cust_dict["email"]
    bill = clean_address(cust_dict["billing_address"])
    ship = clean_address(cust_dict["shipping_address"])
    cust = generate_customer_val(pk, email, username, ship, bill)
    insert(db, "customer", pk, cust)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()
    return pk


def delete_customer(userid):
    """
    Deletes a customer. Will cascade into order, order content, cart item, and payment deletions.
        @param userid : the userid of the customer to delete
        @type userid : integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    delete(db, "customer", userid)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def create_new_supplier(supp_dict):
    """
    Creates a new supplier. Returns the supplier's supplierid
        @param supp_dict a dictionary containing the customer information
        @type supp_dict: a dictionary with dictionaries for addresses
        @return : new supplier's supplierid ad integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    supplier = db["supplier"]
    pk = 1
    if len(supplier) > 0:
        pk = max(supplier.keys()) + 1
    supplier_name = supp_dict["supplier_name"]
    bill = clean_address(supp_dict["billing_address"])
    ship = clean_address(supp_dict["shipping_address"])
    supp = generate_supplier_val(pk, supplier_name, ship, bill)
    insert(db, "supplier", pk, supp)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()
    return pk


def delete_supplier(supplierid):
    """
    Deletes a supplier. Will cascade to product, cart item, order content, order, and payment deletions.
        @param supplierid : the id of the supplier to delete
        @type supplierid : integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    delete(db, "supplier", supplierid)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def add_product(prod_dict):
    """
    Creates a new product. Returns the product's productid
        @param product_name : the product's name
        @type product_name : string
        @param stock : how much stock of the product there is
        @type stock : integer
        @param price : the price of the product
        @type price : number
        @param supplierid : the id of the supplier of this product
        @type supplierid : integer
        @return new product's productid as integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    product = db["product"]
    pk = 1
    if len(product) > 0:
        pk = max(product.keys()) + 1
    product_name = prod_dict["product_name"]
    stock = prod_dict["stock"]
    price = prod_dict["price"]
    supplierid = prod_dict["supplier_id"]
    prod = generate_product_val(pk, product_name, stock, price, supplierid)
    insert(db, "product", pk, prod)

    commit_hypergraph(hypergraph, db)
    hypergraph.close()
    return pk


def place_order(userid, productlist):
    """
    Creates a new order from the user for the products in the product list.
    Quantities randomly generated
    The order's initial status is set to 'placed'
    All ordered items are removed from the cart
        @param userid : the user placing the order
        @type userid : integer
        @param productlist : the list of products the user is ordering.
        @type productlist : array of integers OR a singleton integer
        @return : the order's orderid as an integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    """
    whole_list = False
    try:
        if len(productlist) == 0:
            whole_list = True
    except TypeError:
        productlist = [productlist]
    #Find the cart items from the give userid and productid list
    cart_item = db['cart_item']
    product = db['product']
    user_cart = select(cart_item,f"userid={userid}")
    place_item = user_cart
    if not whole_list:
        place_item = select(user_cart,f"productid${productlist}")
    #determine if all the ordered items are in stock
    for pi in place_item:
        ci = place_item[pi]
        p = product[ci['productid']]
        if(p['stock'] < ci['quantity']):
            hypergraph.close()
            raise Exception(f"{p['product_name']} is out of stock")
            #if not all items in stock, nothing is inserted
    #if they are, create the order
    """
    order = db["order"]
    product = db["product"]
    pk = 1
    if len(order) > 0:
        pk = max(order.keys()) + 1
    new_ord = generate_order_val(pk, "placed", userid)
    insert(db, "order", pk, new_ord)
    # then for each product in the order, create order contents
    for fk in productlist:
        ci = product[fk]
        quantity = min(ci["stock"], random.randint(1, 5))
        feedback = fake.sentence()
        oc = generate_order_content_val(pk, fk, quantity, feedback)
        insert(db, "order_content", (pk, fk), oc)
        # delete(db,'cart_item',(userid,fk))

    commit_hypergraph(hypergraph, db)
    hypergraph.close()
    return pk


def pay_for_order(orderid, userid, value, method):
    """
    Creates a payment from the user for a specific order.
        @param orderid : the order to pay for
        @type orderid : integer
        @param userid : the user paying for the order (the one who placed it)
        @type userid : integer
        @param value : the value being paid
        @type value : number
        @param method : the method of payment
        @type method : string
        @raise Exception: If order status is not 'placed'
        @raise Exception: If user did not place this order
        @raise Exception: if value is not equal to the order's cost
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    order = db["order"]
    status = order[orderid]["order_status"]
    if status != "placed":
        hypergraph.close()
        raise Exception(f"Incorrect order status: {status} should be 'placed'")
    try:
        cost = calc_cost(db, orderid, userid)
    except Exception as err:
        hypergraph.close()
        raise err
    if cost != value:
        hypergraph.close()
        raise Exception(f"Payment value {value} is not equal to order cost {cost}")
    payment = generate_payment_val(orderid, cost, method)
    insert(db, "payment", orderid, payment)
    update(db, "order", orderid, {"order_status": "ordered"})

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def cancel_order(orderid, userid):
    """
    Cancels an order, setting its status to 'cancelled'
        @param orderid : the order to cancel
        @type orderid : integer
        @param userid : the user the order belongs to
        @type userid : integer
        @raise Exception : if the user did not place the order
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    order = db["order"][orderid]
    if order["userid"] == userid:
        update(db, "order", orderid, {"order_status": "cancelled"})
    else:
        raise Exception(
            f"Incorrect user, cannot cancel order {orderid} with user {userid}"
        )

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


"""mass insert"""


def mass_insert_customer(customers):
    """
    Inserts a large number of customers in a single function
        @param customers : all the customer data to insert
        @type customers : a 2D array. Rows are customers, columns are
            0:string:username
            1:string:email
            2:formatted string:shipping_address
            3:formatted stringbilling_address
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    customer = db["customer"]
    pk = 1
    if len(customer) > 0:
        pk = max(customer.keys()) + 1
    for i in range(len(customers)):
        ship = parse_address(customers[i][2])
        bill = parse_address(customers[i][3])
        cust = generate_customer_val(pk, customers[i][1], customers[i][0], ship, bill)
        insert(db, "customer", pk, cust)
        pk += 1

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def mass_insert_supplier(suppliers):
    """
    Inserts a large number of suppliers in a single function
    @param suppliers: all the supplier data to insert
    @type suppliers: a 2D array. Rows are suppliers, columns are
        0:string: supplier_name
        1:formatted string: shipping_address
        2:formatted string: billing_address
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    supplier = db["supplier"]
    pk = 1
    if len(supplier) > 0:
        pk = max(supplier.keys()) + 1
    for i in range(len(suppliers)):
        ship = parse_address(suppliers[i][1])
        bill = parse_address(suppliers[i][2])
        supp = generate_supplier_val(pk, suppliers[i][0], ship, bill)
        insert(db, "supplier", pk, supp)
        pk += 1

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def mass_insert_product(products):
    """
    Inserts a large number of products in a single function
        @param suppliers: all the product data to insert
        @type suppliers: a 2D array. Rows are suppliers, columns are
            0:string: product_name
            1:integer: stock
            2:number: price
            3:integer: supplierid
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    product = db["product"]
    pk = 1
    if len(product) > 0:
        pk = max(product.keys()) + 1
    for i in range(len(products)):
        prod = generate_product_val(
            pk, products[i][0], products[i][1], products[i][2], products[i][3]
        )
        insert(db, "product", pk, prod)
        pk += 1

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


"""updates"""


def change_product_stock(productid, stock):
    """
    Changes the stock of the product to the given number
        @param productid : the product to change the stock of
        @type productid : integer
        @param stock : the number to change the product's stock to
        @type stock : integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    update(db, "product", productid, {"stock": stock})

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def fulfill_order(orderid):
    """
    The suppliers fill the order.
    This reduces their products' stock by the quantity ordered.
    Sets the order status to 'shipped'
        @param orderid : the order to fulfill
        @type orderid : integer
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    product = db["product"]
    order_content = db["order_content"]
    oc = select(order_content, f"orderid={orderid}")
    proj = project("productid,quantity", oc)
    for p in proj:
        v = proj[p]
        pid = v["productid"]
        new_stock = product[pid]["stock"] - v["quantity"]
        update(db, "product", pid, {"stock": new_stock})
    update(db, "order", orderid, {"order_status": "shipped"})

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


def give_order_feedback(userid, productid, orderid, review):
    """
    The user gives a reveiw on a product they ordered.
    Order must have shipped for the user to review it
    Updates the status of the order to 'arrived'.
        @param userid : the user placing the order
        @type userid : integer
        @param orderid : the order the review is based on
        @type orderid : integer
        @param productid : the product the order is for
        @type productid : integer
        @param review : the user's review for the product
        @type review : string
        @raise Exception : if the order status is not shipped
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)

    pk = (orderid, productid)
    order = db["order"][orderid]
    if order["order_status"] != "shipped" and order["order_status"] != "arrived":
        raise Exception(
            f"Order status is {order['order_status']}, order status have be arived shipped"
        )
    if order["userid"] == userid and pk in db["order_content"]:
        update(db, "order_content", pk, {"feedback": review})
        update(db, "order", orderid, {"order_status": "arrived"})

    commit_hypergraph(hypergraph, db)
    hypergraph.close()


"""Searches"""


def search_product():
    """
    Gets all products with available stock
        @return : a list of dictionaries with the product information.
        Keys : {productid, product_name, price, stock, supplier_name}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    product = db["product"]
    supplier = db["supplier"]
    in_stock = select(product, "stock > 0")
    products_and_suppliers = inner_join(in_stock, supplier, "supplierid")
    pas = rename(products_and_suppliers, "supplierid.supplier_name", "supplier_name")
    proj = project("productid,product_name,price,stock,supplier_name", pas)
    return list(proj.values())


def get_items_in_cart(userid):
    """
    Gets all items in the user's cart
        @param userid : the user whose cart to get
        @type userid : integer
        @return : a list of dictionaries with the product and cart information
        Keys : {productid, product_name, price, quantity}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    cart_item = db["cart_item"]
    product = db["product"]
    ci = select(cart_item, f"userid={userid}")
    cip = inner_join(ci, product, "productid")
    cip = rename(cip, "productid.productid", "productid")
    cip = rename(cip, "productid.product_name", "product_name")
    cip = rename(cip, "productid.price", "price")
    cip = project("productid,product_name,price,quantity", cip)
    return list(cip.values())


def get_cost_of_order(orderid, userid):
    """
    Returns the total cost of the order.
    Total cost is SUM(quantity*price) on each product ordered.
        @param orderid : the order to calculate the cost of
        @type orderid : integer
        @param userid : the user who placed the order
        @type userid : integer
        @raise Exception : if the user did not place the order
        @return : the cost of the order as a number
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()
    return calc_cost(db, orderid, userid)


def get_product_feedback(productid):
    """
    Gets all the reviews placed for a product.
        @param productid : the product to get reviews for
        @type productid : integer
        @return a list of reviews as strings
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    product = db["product"]
    sp = select(product, f"productid={productid}")
    ocp = inner_join(db["order_content"], sp, "product_id")
    ocp = project("feedback", ocp)
    ret = list()
    for v in ocp.values():
        ret.append(v["feedback"])
    return ret


def get_all_product_feedback():
    """
    Get the feedback for every product
        @return : a list of dictionaries with the product names and feedback
        Keys : {productid, product_name,feedback}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    content = db["order_content"]
    product = db["product"]
    pf = right_join(content, product, "productid")
    # for pid in product:
    #     oc = select(content, f"productid={pid}")
    #     all_feedback = list()
    #     for ocid in oc:
    #         all_feedback.append(content[ocid]["feedback"])
    #     pf[pid]["feedback"] = all_feedback
    pf = project("productid,product_name,feedback", pf)
    return list(pf.values())


def get_customer(username):
    """
    Gets customer by username
        @param username : the username of the customer
        @type username : string
        @return : a list of dictionaries representing the customers with that username (should be size 1)
        Keys : {userid, username, email, shipping_address, billing_address}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    customer = db["customer"]
    c = select(customer, f"username={username}")
    return list(c.values())


def get_supplier(supplier_name):
    """
    Gets supplier by suppliername
        @param supplier_name : the supplier's name
        @type supplier_name : string
        @return a list of dictionaries representing the suppliers with that supplier_name (should be size 1)
        Keys : {supplierid, supplier_name, shipping_address, billing_address}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    supplier = db["supplier"]
    s = select(supplier, f"supplier_name={supplier_name}")
    return list(s.values())


def get_product(product_name):
    """
    Gets product by product_name
        @param product_name : the product's name
        @type product_name : string
        @return a list of dictionary representing the products with that product_name
        Keys : {productid,product_name,stock,price,supplierid}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    product = db["product"]
    p = select(product, f"product_name={product_name}")
    return list(p.values())


def get_orders(userid):
    """
    Gets all the orders by the user
        @param userid : the user to get the orders from
        @type userid : integer
        @return : a list of dictionaries containing the order information
        Keys : {productid,product_name,price,quantity,orderid,order_status}
    """
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    order = db["order"]
    order_content = db["order_content"]
    product = db["product"]
    o = select(order, f"userid={userid}")
    orderids = list(o.keys())
    # get order contents for each order
    oc = select(order_content, f"orderid${orderids}")
    # get product details for each order contents
    ocp = inner_join(oc, product, "productid")
    # rename to remove flattening dots
    ocp = rename(ocp, "productid.productid", "productid")
    ocp = rename(ocp, "productid.product_name", "product_name")
    ocp = rename(ocp, "productid.price", "price")
    # remove extra fields
    ocp = project("orderid,productid,product_name,price,quantity", ocp)
    # don't need userid anymore
    o = project("orderid,order_status", o)
    # join to get order status
    user_orders = inner_join(ocp, o, "orderid")
    # rename flattening dots
    uo = rename(user_orders, "orderid.orderid", "orderid")
    uo = rename(uo, "orderid.order_status", "order_status")
    return list(uo.values())


def username_by_id(uid):
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    return db["customer"][uid]["username"]


def supplier_name_by_id(sid):
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    return db["supplier"][sid]["supplier_name"]


def product_name_by_id(pid):
    hypergraph = shelve.open("hypergraph")
    db = load_hypergraph(hypergraph)
    hypergraph.close()

    return db["product"][pid]["product_name"]


"""THIS FUNCTION CLEARS THE HYPERGRAPH"""
# init_hypergraph()

"""
test_addr = create_address("123 Sesame St", "New York", "NY", "00001", "United States")
addr_str = address_string(test_addr)

create_new_customer("Grover", "Grover@Sesamest.net",addr_str,addr_str)
create_new_customer("Big Bird", "bbird@Sesamest.net",addr_str,addr_str)
create_new_customer("Elmo", "elmo@Sesamest.net",addr_str,addr_str)
create_new_customer("Ernie", "ernie@Sesamest.net",addr_str,addr_str)
create_new_customer("Bert", "bert@Sesamest.net",addr_str,addr_str)
create_new_customer("Cookie Monster", "cookie@Sesamest.net",addr_str,addr_str)

# cm = get_customer('Cookie Monster')
# print(f"Cookie Monster : {cm}")
# delete_customer(cm[0]['userid'])
# cm = get_customer('Cookie Monster')
# print(f"Deleted Cookie Monster : {cm}")
# print()

create_new_supplier("Grover",addr_str,addr_str)
create_new_supplier("Big Bird",addr_str,addr_str)
create_new_supplier("Elmo",addr_str,addr_str)
create_new_supplier("Ernie",addr_str,addr_str)
create_new_supplier("Bert",addr_str,addr_str)
create_new_supplier("Cookie Monster",addr_str,addr_str)

# bb = get_supplier("Big Bird")
# print(f"Big Bird : {bb}")
# delete_supplier(bb[0]['supplierid'])
# bb = get_supplier("Big Bird")
# print(f"Deleted Big Bird : {bb}")
# print()

add_product("Trash Can",500,5,1)
add_product("Mr Snuffleupagus",1000,10,2)
add_product("Dorthy",100,30,3)
add_product("Eyes",125,2,4)
add_product("IDK",0,3,5)
add_product("Cookie",1,10000,6)

# print(search_product())
# tc = get_product("Trash Can")
# print(f"Trash Can : {tc}")
# delete_supplier(1)
# tc = get_product("Trash Can")
# print(f"Cascade deleted Trashcan : {tc}")
# print()


add_item_to_cart(1,1,1)
add_item_to_cart(1,2,1)
add_item_to_cart(1,3,1)
add_item_to_cart(2,4,1)
add_item_to_cart(3,4,1)
add_item_to_cart(2,6,1)

# print(get_items_in_cart(1))
# print()
# remove_item_from_cart(1, 2)
# print(get_items_in_cart(1))
# print()

place_order(1)
gc = get_items_in_cart(1)
print(f"Grover's cart after placing order : {gc}")
print()
o = get_orders(1)
print(f"Orders for Grover : {o}")
print()
# cancel_order(1,1)
# o = get_orders(1)
# print(f"Orders for Grover : {o}")
# print()
cost = get_cost_of_order(1,1)
pay_for_order(1,1,cost,"Credit Card")
o = get_orders(1)
print(f"Orders for Grover (paid): {o}")
print()
fulfill_order(1)
o = get_orders(1)
print(f"Orders for Grover (shipped): {o}")
print()
give_order_feedback(1, 1, 1, "very good")
give_order_feedback(1, 2, 1, "excellent")
give_order_feedback(1, 3, 1, "supurb")
apf = get_all_product_feedback()
print(f"All product feedback : {apf}")
"""
