import mysql.connector
import production.inventory_database as inventory_database
import requests
from production.credentials import db_credentials as credentials

## add_inventory_listings: Adds ecommerce listing from inventory item => Success(bool)


def add_inventory_listings(venue_listings, company_id):

    is_success = db_add_inventory_listings(venue_listings, company_id)
    
    if is_success:
    
        inv_id = venue_listings[0]['inv_id']
        secret = "YOUR_SECRET_TOKEN"
        res = requests.get(f"https://tokyo-english-bookshelf.ngrok.io/api/revalidate?inv_id={inv_id}&secret={secret}")
        
        return True
        
    return False
    
    
def db_add_inventory_listings(listing_details, company_id):

    print(listing_details)

    if listing_details is None or len(listing_details) == 0:
        return None

    try:
    
        connection = mysql.connector.connect(**credentials)
        cursor = connection.cursor(dictionary=True)
        connection.start_transaction()

        insert_data = [(i["inv_id"], i["venue_id"], i["inv_location_id"], i["available"], i["price"], i["ccy_code"], i["featured_id"], i["template_id"]) for i in listing_details]
    
        delete_params = [item for data in insert_data for item in data[:3]]
        delete_placeholders = ', '.join(['(%s, %s, %s)'] * len(insert_data))
        
        delete_query = f"""
        DELETE FROM ecomm_venue_listings
        WHERE (inv_id, venue_id, inv_location_id) IN ({delete_placeholders})
        """
        # Execute DELETE
        cursor.execute(delete_query, delete_params)
        
        insert_query = """
        INSERT INTO ecomm_venue_listings 
        (inv_id, venue_id, inv_location_id, available, price, ccy_code, featured_id, template_id, publish_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        
        cursor.executemany(insert_query, insert_data)

        pub_query_params = [item for data in insert_data for item in data[:2]]
        pub_query_placeholders = ', '.join(['(%s, %s)'] * len(insert_data))
        
        #get pub_ids before deleting
        pub_id_query = f"""
                       SELECT pub_id, inv_id, venue_id from ecomm_pub_inv_map
                       WHERE (inv_id, venue_id) IN ({pub_query_placeholders})
                       """

        cursor.execute(pub_id_query, pub_query_params)
        existing_pub_id_list = cursor.fetchall()
        
        print(existing_pub_id_list)
        
        existing_pub_id_map = {f"{row['inv_id']}_{row['venue_id']}": row["pub_id"] for row in existing_pub_id_list}
        
        num_required_ids = len(insert_data) - len(existing_pub_id_list)
        
        if num_required_ids > 0: 
            pub_range = cursor.callproc('ecomm_reserve_pub_id_range', [num_required_ids, 0, 0])
            print(pub_range)
            start_pub_id = pub_range['ecomm_reserve_pub_id_range_arg2']
            end_pub_id = pub_range['ecomm_reserve_pub_id_range_arg3']
            pub_id = start_pub_id
        else:
            pub_id = 0
            
        ecomm_map_data = []
        ecomm_description_data = []
        del_ecomm_map_data = []
        del_ecomm_description_data = []
        
        for i in listing_details:
            check_key = f"{i['inv_id']}_{i['venue_id']}"
            if check_key in existing_pub_id_map:
                 existing_pub_id = existing_pub_id_map[check_key]
                 del_ecomm_map_data.append((existing_pub_id, i["inv_id"], i["venue_id"]))
                 del_ecomm_description_data.append((existing_pub_id,))
                 ecomm_map_data.append((existing_pub_id, i["inv_id"], i["venue_id"], 1, company_id))
                 ecomm_description_data.append((existing_pub_id, i["description"]))
            else:
                ecomm_map_data.append((pub_id, i["inv_id"], i["venue_id"], 1, company_id))
                ecomm_description_data.append((pub_id, i["description"]))
                pub_id += 1
        
        print(ecomm_map_data)
        
        if len(del_ecomm_map_data) > 0:
            flat_del_ecomm_map_data = [item for tup in del_ecomm_map_data for item in tup]
            delete_placeholders = ', '.join(['(%s, %s, %s)'] * len(insert_data))

            delete_query = f"""
            DELETE FROM ecomm_pub_inv_map
            WHERE (pub_id, inv_id, venue_id) IN ({delete_placeholders})
            """
            cursor.execute(delete_query, flat_del_ecomm_map_data)
    
        insert_query = """
        INSERT INTO ecomm_pub_inv_map(pub_id, inv_id, venue_id, pub_type_id, company_id)
        VALUES (%s, %s, %s, %s, %s)    
        """
        cursor.executemany(insert_query, ecomm_map_data)
        
        if len(del_ecomm_map_data) > 0:
            flat_del_ecomm_description_data = [item for tup in del_ecomm_description_data for item in tup]
            delete_placeholders = ', '.join([('%s')] * len(insert_data))

            delete_query = f"""
            DELETE FROM ecomm_pub_description
            WHERE pub_id in ({delete_placeholders})
            """
            print(delete_query)
            print(flat_del_ecomm_description_data)
        
            cursor.execute(delete_query, flat_del_ecomm_description_data)
    
        insert_query = """
        INSERT INTO ecomm_pub_description(pub_id, description)
        VALUES (%s, %s)    
        """
        cursor.executemany(insert_query, ecomm_description_data)

        connection.commit()
        count = cursor.rowcount

        cursor.close()
        connection.close()

    except mysql.connector.Error as err:
        print(f"❌ Transaction failed: {err}")
        if connection.is_connected():
            connection.rollback()
            cursor.close()
            connection.close()
            return None
                                
    return True 
    
    
