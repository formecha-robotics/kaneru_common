from multiprocessing import Process
import production.database_commands as db
from datetime import datetime
import production.redis_commands as cache
import base64
import os
import hashlib
import time
import production.inventory_database as db
from production.cache_keys import keys_and_policy as kp
from production.constants import kaneru_params

status_codes = { 'QU' : 'Queued', 'EX' : 'Executing', 'OK' : 'Completed', 'KO' : 'Failed' }
job_types = { 'PRICER' : 'price_', 'DESCRIPTION_GENERATOR' : 'description_', 'CATEGORY_GENERATOR' : 'cat_suggestion_', 'NORMALIZER' : 'normalizer_' }

def gen_submit_id(job_type, token):

    if job_type is None or token is None:
        return None
        
    prefix = job_types.get(job_type, None)
    if prefix is None:
        print(f"kaneru_job_launcher: ERROR, unsupported job type, {job_type}")
        return None
    data = prefix + token
    sha256_hash = hashlib.sha256(data.encode('utf-8')).digest()
    return sha256_hash[:8]

def queue_job(job_type, token, target, params, description):

    submit_id = gen_submit_id(job_type, token)
    if submit_id is not None:
        args = (submit_id,) + params
        
        queue_event = { 'job_type' : job_type, 'description' : description, 'status' : status_codes['QU'], "completed_at" : None}
        submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
        redis_key = kp["SYSTEM_JOB"]["key_prefix"] + submit_id_str
        status = cache.write_json(redis_key, queue_event)
        #expiry_min = kp["SYSTEM_JOB"]["expiry_policy"]
        
        if status:
            return launch(submit_id, target=target, args=args)
        else:
            return False
    else:
        return False
            
    return True
    
def is_running(job_type, token):    

    submit_id = gen_submit_id(job_type, token)
    if submit_id is not None:
        submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
        redis_key = kp["SYSTEM_JOB"]["key_prefix"] + submit_id_str
        expiry_min = kp["SYSTEM_JOB"]["expiry_policy"]
        status, job_details = cache.find_valid_json(redis_key, expiry_min)
        if status:
            status_code = job_details['status']
            if status_code == "Executing":
                return True
            
    return False            
                
def wait_job(job_type, token, timeout_seconds=180):

    submit_id = gen_submit_id(job_type, token)
    print(submit_id)
   
    if submit_id is not None:
        submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")
        print(submit_id_str)
        redis_key = kp["SYSTEM_JOB"]["key_prefix"] + submit_id_str
        expiry_min = kp["SYSTEM_JOB"]["expiry_policy"]
        status, job_details = cache.find_valid_json(redis_key, expiry_min)
        if status:
            status_code = job_details['status']
            time_seconds = 0
            while status_code != 'Completed' and status_code != 'Failed' and time_seconds <= timeout_seconds:
                time.sleep(2)
                _, job_details = cache.find_valid_json(redis_key, expiry_min)
                status_code = job_details['status']
                time_seconds += 2
                
            if status_code == 'Completed':
                return True
            else:
                if time_seconds > timeout_seconds:
                    print("ERROR: job timed out");
                else:
                    print("ERROR: job failed")    
                return False
                
    return False

def mark_job(submit_id, status_code):

    if status_code == 'QU':
        insert_sql = f"""
        INSERT INTO kaneru_system_job_queue 
        (submit_id, status_code)
        VALUES (%s, %s)
        """
        insert_data = (submit_id, status_code,)
    
        count = db.single_insert(insert_sql, insert_data)
    
        if count is None or count!=1:
            return False
    elif status_code == 'EX' or status_code == 'OK' or status_code == 'KO':
   
        update_sql = f"""
        UPDATE kaneru_system_job_queue 
        SET status_code = %s
        WHERE submit_id = %s
        """  
        
        update_data = (status_code, submit_id,)
        
        count = db.execute_query(update_sql, update_data)

        if count is None or count!=1:
            return False
        
        submit_id_str = base64.urlsafe_b64encode(submit_id).decode('utf-8').rstrip("=")   
        redis_key = kp["SYSTEM_JOB"]["key_prefix"] + submit_id_str
        expiry_min = kp["SYSTEM_JOB"]["expiry_policy"]
        status, job_details = cache.find_valid_json(redis_key, expiry_min)
        if status:
            job_details['status'] = status_codes[status_code]
            if status_code == 'OK' or status_code == 'KO':
                job_details['completed_at'] = int(time.time())
            status = cache.update_json(redis_key, job_details)
            if not status:
                print("Failed to mark job as complete")
                return False 
        else:
            print("Redis key error, Failed to mark job as complete")
            return False
    else:
        print(f"ERROR: invalid job status {status_code}");
        return False      
        
    return True

def launch(submit_id, target, args):

    p = Process(target=target, args=args)
    status = mark_job(submit_id, 'QU')
    if status:
        p.start()
    
    return status
    
    
    
    
