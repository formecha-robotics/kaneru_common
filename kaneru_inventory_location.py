import production.inventory_database as db
import production.mysql_helper as sql_helper
import uuid
import mysql.connector
import hashlib
import time

def locations_list(company_id):
    db_query = f"""
    SELECT count(i.quantity) as quantity, m.location, m.sublocation 
    FROM inv_location_mapping m, inv_location_stock i
    WHERE m.company_id = {company_id}
    AND m.inv_location_id = i.inv_location_id
    AND i.quantity <> 0
    GROUP BY m.inv_location_id, m.location, m.sublocation
    """
    inventories_f = db.execute_query(db_query)
    
    used_locations = {(item['location'].lower(), item['sublocation'].lower()) for item in inventories_f}
    
    missing_query = f"""
    SELECT 0 as quantity, m.location, m.sublocation 
    FROM inv_location_mapping m
    WHERE m.company_id = {company_id}
    """
    inventories_e = db.execute_query(missing_query)
    
    filtered_inventories_e = [item for item in inventories_e if (item['location'].lower(), item['sublocation'].lower()) not in used_locations]
    
    inventories_l = inventories_f + filtered_inventories_e 
    
    inventories = {};
    for i in inventories_l:
       location = i['location']
       sublocation = i['sublocation']
       quantity = i['quantity']
       if not location in inventories.keys():
           inventories[location] = []
       inventories[location].append({'sublocation' : sublocation, 'quantity':quantity})
           
    return inventories

def rename_sublocation(location, old_sublocation, sublocation, company_id):
    db_query = f"""
    UPDATE inv_location_mapping
    SET sublocation = %s  
    WHERE company_id = %s AND location = %s AND sublocation = %s
    """
    
    params = (sql_helper.sanitize(sublocation), company_id, sql_helper.sanitize(location), sql_helper.sanitize(old_sublocation))
        
    count = db.execute_query(db_query, params)
    
    if count==1:
        return True
    else:
        return False
        
def rename_location(old_location, location, company_id):
    db_query = f"""
    UPDATE inv_location_mapping
    SET location = %s  
    WHERE company_id = %s AND location = %s
    """
    
    params = (sql_helper.sanitize(location), company_id, sql_helper.sanitize(old_location))
        
    count = db.execute_query(db_query, params)
    
    if count > 0:
        return True
    else:
        return False
        
def add_location(location, sublocation, company_id):
    db_query = f"""
    INSERT INTO inv_location_mapping(location, sublocation, company_id, loc_unique_id)
    VALUES(%s, %s, %s, %s)
    """
    
    loc_unique_id_str = f"{location}_{sublocation}_{int(time.time() * 1000)}"
    loc_unique_id = hash_bytes = hashlib.sha256(loc_unique_id_str.encode('utf-8')).digest()[:8]
            
    params = (sql_helper.sanitize(location), sql_helper.sanitize(sublocation), company_id, loc_unique_id)
    
    result = db.execute_query(db_query, params)
    
    if result is None or result==0: 
        return False
    
    return True
    
def move_sublocation(location, new_location, sublocation_list, company_id):

    sublocation_placeholder = ', '.join(['%s'] * len(sublocation_list))

    # 1st, make sure there is no conflict with the sublocations already existing in the new location, return False if so!
    db_query = f"""
    SELECT sublocation FROM inv_location_mapping
    WHERE location = %s
    AND company_id = %s
    AND sublocation in ({sublocation_placeholder})
    """
    
    sanitized_location = sql_helper.sanitize(location)
    sanitized_new_location = sql_helper.sanitize(new_location)
    sanitized_sub_locations = tuple(sql_helper.sanitize(x) for x in sublocation_list)
    
    params = (sanitized_new_location, ) + (company_id,) + sanitized_sub_locations
    
    result = db.execute_query(db_query, params)
    
    if len(result) > 0:
        return False
        
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='inventory_user',
            password='StrongPassword123!',
            database='product_inventory'
        )
        
        try:
        
            if conn.is_connected():
            
                cursor = conn.cursor()
                conn.start_transaction()
                
                # 2nd, create the new sublocations in the new location
                placeholders = []
                params = []
                for sublocation in sanitized_sub_locations:
                    placeholders.append("(%s, %s, %s)")
                    params.extend([sanitized_location, sublocation, company_id])

                # Build one SELECT FROM with a WHERE IN clause
                cursor.execute(f"""
                INSERT INTO inv_location_mapping (location, sublocation, company_id, loc_unique_id)
                SELECT %s, sublocation, company_id, loc_unique_id
                FROM inv_location_mapping
                WHERE (location, sublocation, company_id) IN ({','.join(placeholders)})
                """, [sanitized_new_location] + params)
    
                # 3rd, copy all the inventory items to their new inventory_location_ids to a temporary table     
                temp_table_name = f"temp_{uuid.uuid4().hex[:8]}"
                
                params = (sanitized_location, sanitized_new_location, company_id,) + sanitized_sub_locations
                
                cursor.execute(f"CREATE TEMPORARY TABLE {temp_table_name} AS "\
                    "SELECT i.inv_id, o.inv_location_id as old_inv, n.inv_location_id, i.quantity, "\
                    "i.reserved, i.allocated, i.expire_date, i.last_update, i.recovery_status "\
                    "FROM inv_location_stock i, inv_location_mapping n, inv_location_mapping o "\
                    "WHERE o.inv_location_id = i.inv_location_id "\
                    "AND o.location = %s "\
                    "AND n.location = %s "\
                    "AND o.company_id = %s "\
                    "AND o.company_id = n.company_id "\
                    f"AND o.sublocation in ({sublocation_placeholder}) "\
                    "AND o.sublocation = n.sublocation", params)
    
                #4th, delete entries from inv_location_stock which are linked to the old sublocations
                cursor.execute(f"DELETE i FROM inv_location_stock i "\
                    f"JOIN {temp_table_name} t ON i.inv_id = t.inv_id AND i.inv_location_id = t.old_inv", ())
        
                #5th, inserts the new values into inv_location_stock
                cursor.execute("INSERT INTO inv_location_stock (inv_id, inv_location_id, quantity, "\
                    "reserved, allocated, expire_date, last_update, recovery_status) "\
                    "SELECT inv_id, inv_location_id, quantity, reserved, allocated, expire_date, "\
                    "last_update, recovery_status "\
                    f"FROM {temp_table_name}", ())

                #6th, delete sublocations from original location
                params = (sanitized_location, ) + (company_id,) + sanitized_sub_locations
                
                cursor.execute("DELETE FROM inv_location_mapping "\
                    "WHERE location = %s "\
                    "AND company_id = %s "\
                    f"AND sublocation in ({sublocation_placeholder})", params)
    
                conn.commit()
                                
        except Exception as e:
            conn.rollback()
            print("Transaction error:", e)
            return False
        finally:
            cursor.close()
            conn.close()    

    except Error as e:
        print("Connection error:", e)           
        return False
    
    return True
            
def delete_location(location, sublocation_list, company_id):
    
    sublocation_placeholder = ', '.join(['%s'] * len(sublocation_list))
    
    db_query = f"""
    SELECT sum(quantity) AS total_items, m.sublocation
    FROM inv_location_mapping m, inv_location_stock i
    WHERE m.inv_location_id = i.inv_location_id
    AND m.location = %s 
    AND m.sublocation IN ({sublocation_placeholder})
    AND m.company_id = %s
    GROUP BY m.sublocation
    HAVING total_items > 0
    """
    
    sanitized_location = sql_helper.sanitize(location)
    sanitized_sub_locations = tuple(sql_helper.sanitize(x) for x in sublocation_list)

    
    params = (sanitized_location, ) + sanitized_sub_locations + (company_id,)
    
    result = db.execute_query(db_query, params)
        
    exclude = [item['sublocation'] for item in result]
    sanitized_sub_locations = [item for item in sanitized_sub_locations if item not in exclude]

    sublocation_placeholder = ', '.join(['%s'] * len(sanitized_sub_locations))
    sanitized_sub_locations = tuple(x for x in sanitized_sub_locations)
    params = (sanitized_location, ) + sanitized_sub_locations + (company_id,)
    
    delete_cmd = f"""
    DELETE FROM inv_location_mapping
    WHERE location = %s 
    AND sublocation in ({sublocation_placeholder})
    AND company_id = %s
    """
        
    if len(sanitized_sub_locations) > 0:
        delete_result = db.execute_query(delete_cmd, params)
        if delete_result is not None and delete_result > 0:
            return True
        else:
            return False
    else:  
        return False

    return True

def move_inventory_items_to_location(inv_id_map, company_id, location, sublocation):  
    if not inv_id_map:
        return False  # Nothing to do
        
    #firstly filter out any copy requests from the destination to the destination
    filtered = [item for item in inv_id_map if not (item["location"] == location and item["sublocation"] == sublocation)]
    
    if len(filtered) ==0 :
        return True

    #next divide the list up into those with a quantity field and those without as db update is different
    with_quantity = [item for item in filtered if "quantity" in item]
    without_quantity = [item for item in filtered if "quantity" not in item]
    
    with_status = True
    without_status = True
    
    if len(without_quantity) > 0:
        #for the simple case without quantity we need to find out if the inv_id is already in the destination, meaning
        #this it is a consoildate rather than a move 
        inv_ids = [item['id'] for item in without_quantity]
        destination_ids = get_items_in_inventory_destination(inv_ids, company_id, location, sublocation) 
    
        for_consolidation = [item for item in without_quantity if item['id'] in [ditem['inv_id'] for ditem in destination_ids]]
        simple_case = [item for item in without_quantity if item['id'] not in [ditem['inv_id'] for ditem in destination_ids]]
        print(f"simple case: {simple_case}")
        
        simple_status = True
        consolidation_status = True
        
        if len(simple_case) > 0:
            simple_case_no_duplicates = [] 
            id_list = []
            for item in simple_case:
                if item['id'] not in id_list:
                    id_list.append(item['id'])
                    simple_case_no_duplicates.append(item) #if there is a duplicate add the first one to simple
                else:
                    for_consolidation.append(item) #treat the others like consolidations    
                    
            print(f"simple case no dupes: {simple_case_no_duplicates}")
            print(f"for consolidation: {for_consolidation}")
        
            if len(simple_case_no_duplicates) > 0:
        
                #inv_ids = [item['id'] for item in simple_case_no_duplicates]
  
                simple_status = simple_move_inventory_items_to_location(simple_case_no_duplicates, company_id, location, sublocation)
  
        if len(for_consolidation) > 0 and simple_status:
            print(f"for consolidation: {for_consolidation}")
            consolidation_status = consolidate_inventory_items_to_location(for_consolidation, company_id, location, sublocation)
            
        without_status = (consolidation_status and simple_status)
      
    if len(with_quantity) > 0:
 
        inv_ids = [item['id'] for item in with_quantity]
        destination_ids = get_items_in_inventory_destination(inv_ids, company_id, location, sublocation) 
    
        for_quantity_consolidation = [item for item in with_quantity if item['id'] in [ditem['inv_id'] for ditem in destination_ids]]
        simple_quantity_case = [item for item in with_quantity if item['id'] not in [ditem['inv_id'] for ditem in destination_ids]]
        print(f"simple quantity case: {simple_quantity_case}")
        
        simple_quantity_status = True
        consolidation_quantity_status = True
        
        if len(simple_quantity_case) > 0:
            simple_quantity_case_no_duplicates = [] 
            id_list = []
            for item in simple_quantity_case:
                if item['id'] not in id_list:
                    id_list.append(item['id'])
                    simple_quantity_case_no_duplicates.append(item) #if there is a duplicate add the first one to simple
                else:
                    for_quantity_consolidation.append(item) #treat the others like consolidations  

            print(f"simple quantity case no dupes: {simple_quantity_case_no_duplicates}")
            print(f"for quantity consolidation: {for_quantity_consolidation}")

            if len(simple_quantity_case_no_duplicates) > 0:
            
                simple_quantity_status = move_quantity_inventory_items_to_location(simple_quantity_case_no_duplicates, company_id, location, sublocation, False)
                
                
        if len(for_quantity_consolidation) > 0 and simple_quantity_status:
            print(f"for quantity consolidation: {for_quantity_consolidation}")
            consolidation_quantity_status = move_quantity_inventory_items_to_location(for_quantity_consolidation, company_id, location, sublocation, True)
        
        with_status = (consolidation_quantity_status and simple_quantity_status)
                
            
    return (with_status and without_status)

def get_inv_location_ids_with_qty(inv_dict_list, company_id, host='localhost', user='inventory_user', password='StrongPassword123!', database='product_inventory', port=3306):

    config = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port
    }

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        cursor.execute("""
           CREATE TEMPORARY TABLE tmp_inv_move (
                inv_id INT,
                company_id INT,
                location VARCHAR(255),
                sublocation VARCHAR(255),
                quantity DOUBLE
            );
        """)

        insert_values = [
            (item['id'], company_id, item['location'], item['sublocation'], item['quantity'])
            for item in inv_dict_list
        ]

        cursor.executemany("""
            INSERT INTO tmp_inv_move (inv_id, company_id, location, sublocation, quantity)
            VALUES (%s, %s, %s, %s, %s);
        """, insert_values)

        cursor.execute("""
            SELECT t.inv_id, m.inv_location_id, t.quantity
            FROM tmp_inv_move t
            JOIN inv_location_mapping m
              ON t.company_id = m.company_id
              AND t.location = m.location
              AND t.sublocation = m.sublocation;
        """)

        results = cursor.fetchall()
        cursor.close()
        connection.close()

        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

        
def get_inv_location_ids(inv_dict_list, company_id, host='localhost', user='inventory_user', password='StrongPassword123!', database='product_inventory', port=3306):

    config = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port
    }

    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        cursor.execute("""
           CREATE TEMPORARY TABLE tmp_inv_move (
                inv_id INT,
                company_id INT,
                location VARCHAR(255),
                sublocation VARCHAR(255)
            );
        """)

        insert_values = [
            (item['id'], company_id, item['location'], item['sublocation'])
            for item in inv_dict_list
        ]

        cursor.executemany("""
            INSERT INTO tmp_inv_move (inv_id, company_id, location, sublocation)
            VALUES (%s, %s, %s, %s);
        """, insert_values)

        cursor.execute("""
            SELECT t.inv_id, m.inv_location_id
            FROM tmp_inv_move t
            JOIN inv_location_mapping m
              ON t.company_id = m.company_id
              AND t.location = m.location
              AND t.sublocation = m.sublocation;
        """)

        results = cursor.fetchall()
        cursor.close()
        connection.close()

        return results

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_inventory_locations_for_items(inv_id_map, company_id):
    
    placeholders = ','.join(['%s'] * len(inv_id_map))
    
    db_query = f"""
    SELECT DISTINCT m.location, m.sublocation 
    FROM inv_location_stock s, inv_location_mapping m 
    WHERE m.inv_location_id = s.inv_location_id
    AND s.inv_id in ({placeholders})
    and m.company_id= %s;
    """ 
    
    inv_ids = [item['id'] for item in inv_id_map]
        
    params = tuple(inv_ids) + (company_id,)
    
    location_details_l = db.execute_query(db_query, params)
    
    return location_details_l
    
def get_items_in_inventory_destination(inv_ids, company_id, location, sublocation):

    placeholders = ','.join(['%s'] * len(inv_ids))
    query = f"""
    select s.inv_id
    FROM inv_location_stock s, inv_location_mapping m
    WHERE m.company_id = %s AND m.location = %s AND m.sublocation = %s
    AND s.inv_location_id = m.inv_location_id
    AND s.inv_id IN ({placeholders})
    """
    params = (company_id, location, sublocation) + tuple(inv_ids,)
    
    try:
        return db.execute_query(query, params) 

    except Exception as e:
        print(f"[ERROR] Failed to update inventory locations: {e}")
        return None


def consolidate_inventory_items_to_location(inv_dict_list, company_id, location, sublocation,  host='localhost', user='inventory_user', password='StrongPassword123!', database='product_inventory', port=3306):
   
    query_pairs = get_inv_location_ids(inv_dict_list, company_id)
       
    if not query_pairs:
        print("[INFO] No valid (inv_id, current_location_id) pairs found.")
        return False

    config = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": port
    }

    try:
    
        tablename_source = f"temp_{uuid.uuid4().hex[:8]}"
        tablename_destination = f"temp_{uuid.uuid4().hex[:8]}"
        tablename_consolidated = f"temp_{uuid.uuid4().hex[:8]}"
    
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()

        placeholders = ', '.join(['(%s, %s)'] * len(query_pairs))
        flat_values = [val for tup in query_pairs for val in tup]
        
        db_query = f"""
        CREATE TEMPORARY TABLE {tablename_source} 
        SELECT inv_id, inv_location_id, quantity, reserved, allocated  
        FROM inv_location_stock
        WHERE (inv_id, inv_location_id) IN ({placeholders});
        """      

        cursor.execute(db_query, flat_values)
        
        inv_ids = [item['id'] for item in inv_dict_list]       
        placeholders2 = ', '.join(['%s'] * len(inv_ids))

        db_query = f"""
        CREATE TEMPORARY TABLE {tablename_destination} 
        SELECT s.inv_id, s.inv_location_id, s.quantity, s.reserved, s.allocated  
        FROM inv_location_stock s, inv_location_mapping m
        WHERE s.inv_location_id = m.inv_location_id
        AND m.company_id = %s
        AND m.location = %s
        AND m.sublocation = %s
        AND s.inv_id IN ({placeholders2});
        """ 
        
        params = (company_id, location, sublocation) + tuple(inv_ids,)  
        
        cursor.execute(db_query, params)
            
        db_query = f"""
        CREATE TEMPORARY TABLE {tablename_consolidated} 
        SELECT inv_id, SUM(quantity) AS quantity, SUM(reserved) AS reserved, SUM(allocated) AS allocated
        FROM (
            SELECT inv_id, quantity, reserved, allocated FROM {tablename_source}
            UNION ALL
            SELECT inv_id, quantity, reserved, allocated FROM {tablename_destination}
	    ) AS combined
        GROUP by inv_id;
        """

        cursor.execute(db_query)
    
        db_query = f"""
        UPDATE {tablename_destination} d
        JOIN {tablename_consolidated} c ON d.inv_id = c.inv_id
        SET 
            d.quantity = c.quantity,
            d.reserved = c.reserved,
            d.allocated = c.allocated;
        """

        cursor.execute(db_query)
    
        db_query = f"""
        DELETE FROM inv_location_stock s 
        WHERE (inv_id, inv_location_id) IN (SELECT inv_id, inv_location_id FROM {tablename_destination})
        """    
       
        cursor.execute(db_query)

        db_query = f"""
        DELETE FROM inv_location_stock s 
        WHERE (inv_id, inv_location_id) IN ({placeholders})
        """    
       
        cursor.execute(db_query, flat_values)

    
        db_query = f"""
        INSERT INTO inv_location_stock (inv_id, inv_location_id, quantity, reserved, allocated)
        SELECT inv_id, inv_location_id, quantity, reserved, allocated
        FROM {tablename_destination};
        """
        
        cursor.execute(db_query)
        
        cursor.close()
        connection.commit()
        connection.close()
        return True

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return False

def move_quantity_inventory_items_to_location(inv_dict_list, company_id, location, sublocation, does_dest_exists, host='localhost', user='inventory_user', password='StrongPassword123!', database='product_inventory', port=3306): 

    query_qpairs = get_inv_location_ids_with_qty(inv_dict_list, company_id)
   
    if not query_qpairs:
        print("[INFO] No valid (inv_id, current_location_id) pairs found.")
        return False

                
    # Create composite WHERE clause: (inv_id, current_location_id)
    placeholders = ', '.join(['(%s, %s)'] * len(query_qpairs))
    flat_values = [val for tup in query_qpairs for val in tup[0:2]]

    db_query = f"""
    SELECT * FROM inv_location_stock
    WHERE (inv_id, inv_location_id) IN ({placeholders});
    """

    params = tuple(flat_values)
        
    try:
        existing_quantity = db.execute_query(db_query, params)
        
        move_map = {}
        update_tuple = []
        for item in existing_quantity:
            inv_id = item['inv_id']
            inv_location_id = item['inv_location_id']
            quantity = item['quantity']
            allocated = item['allocated']
            reserved = item['reserved']
        
            move_quantity = [item for item in query_qpairs if (item[0]==inv_id and item[1]==inv_location_id)][0][2]
            
            move_reserved = reserved if (reserved - move_quantity) <= 0 else move_quantity #move everything available
            if (move_reserved < move_quantity):
                 move_allocated = allocated if (allocated - (move_quantity + move_reserved)) <=0 else (move_quantity - move_reserved)
            else:
                 move_allocated = 0
                 
            new_quantity = quantity - move_quantity
            new_reserved = reserved - move_reserved
            new_allocated = allocated - move_allocated
            
            if not inv_id in move_map.keys():
                move_map[inv_id] = {'quantity' : 0, 'reserved' : 0, 'allocated' : 0}
            move_map[inv_id]['quantity'] += move_quantity
            move_map[inv_id]['reserved'] += move_reserved
            move_map[inv_id]['allocated'] += move_allocated
            
            update_tuple.append((inv_id, inv_location_id, new_quantity, new_allocated, new_reserved))
    
        print(update_tuple)
        
        move_tuple = [(inv_id, move_map[inv_id]['quantity'], move_map[inv_id]['reserved'], move_map[inv_id]['allocated']) for inv_id in move_map.keys() ]
        
        print(move_tuple)
        
        
        config = {
            "host": host,
            "user": user,
            "password": password,
            "database": database,
            "port": port
        }
        
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        tablename_source = f"temp_{uuid.uuid4().hex[:8]}"
        
        db_query = f"""
        CREATE TEMPORARY TABLE {tablename_source} (inv_id INT, inv_location_id INT, quantity DOUBLE, reserved DOUBLE, allocated DOUBLE)
        """
        print(db_query)
        
        cursor.execute(db_query)
        
        placeholders2 = ', '.join(['%s, %s, %s, %s, %s'] * len(update_tuple))

        db_query = f"""
        INSERT INTO {tablename_source} VALUES({placeholders2})
        """
        print(db_query)
        
        cursor.executemany(db_query, update_tuple)
        
        db_query = f"""
        UPDATE inv_location_stock s
        JOIN {tablename_source} t 
        ON s.inv_id = t.inv_id
        AND s.inv_location_id = t.inv_location_id
        SET 
            s.quantity = t.quantity,
            s.reserved = t.reserved,
            s.allocated = t.allocated;
        """
        
        print(db_query)
        
        cursor.execute(db_query)
        
        tablename_dest = f"temp_{uuid.uuid4().hex[:8]}"
        
        db_query = f"""
        CREATE TEMPORARY TABLE {tablename_dest} (inv_id INT, quantity DOUBLE, reserved DOUBLE, allocated DOUBLE)
        """
        print(db_query)
        
        cursor.execute(db_query)
        
        placeholders3 = ', '.join(['%s, %s, %s, %s'] * len(move_tuple))
        
        db_query = f"""
        INSERT INTO {tablename_dest} VALUES({placeholders3})
        """
        
        print(db_query)
        
        cursor.executemany(db_query, move_tuple)
        
        if does_dest_exists:
        
            db_query = f"""
            UPDATE inv_location_stock s
            JOIN {tablename_dest} t 
            ON s.inv_id = t.inv_id
            SET 
                s.quantity = s.quantity + t.quantity,
                s.reserved = s.reserved + t.reserved,
                s.allocated = s.allocated + t.allocated
            WHERE s.inv_location_id in (
                SELECT inv_location_id 
                FROM inv_location_mapping
                WHERE location = %s
                AND sublocation = %s
                AND company_id = %s
                );
            """
            
            params = (location, sublocation, company_id)
        
            print(db_query)
            
            cursor.execute(db_query, params)
        

        else:
        
            db_query = f"""
            INSERT INTO inv_location_stock (inv_id, quantity, reserved, allocated, inv_location_id)
            SELECT d.inv_id, d.quantity, d.reserved, d.allocated, (
                SELECT inv_location_id 
                FROM inv_location_mapping 
                WHERE location = %s 
                AND sublocation = %s 
                AND company_id = %s
                LIMIT 1
            ) AS target_location_id
            FROM {tablename_dest} d;
            """        

            params = (location, sublocation, company_id)
  
            print(db_query)
            
            cursor.execute(db_query, params)
        cursor.close()
        connection.commit()
        connection.close()
        return True
       
    except Exception as e:
        print(f"[ERROR] Failed to update inventory locations: {e}")
        return False

    return True
   
def simple_move_inventory_items_to_location(inv_dict_list, company_id, location, sublocation):
   
    query_pairs = get_inv_location_ids(inv_dict_list, company_id)
   
    if not query_pairs:
        print("[INFO] No valid (inv_id, current_location_id) pairs found.")
        return False
    
    # Create composite WHERE clause: (inv_id, current_location_id)
    placeholders = ', '.join(['(%s, %s)'] * len(query_pairs))
    flat_values = [val for tup in query_pairs for val in tup]

    db_query = f"""
    UPDATE inv_location_stock
    SET inv_location_id = (
        SELECT inv_location_id FROM inv_location_mapping
        WHERE company_id = %s AND location = %s AND sublocation = %s
    )
    WHERE (inv_id, inv_location_id) IN ({placeholders});
    """

    params = (company_id, location, sublocation) + tuple(flat_values)

    try:
        db.execute_query(db_query, params)
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to update inventory locations: {e}")
        return False

