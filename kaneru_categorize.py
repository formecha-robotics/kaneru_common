import faiss
import base64
import numpy as np
from production.cache_keys import keys_and_policy as kp
from production.redis_commands import write_json
from production.redis_commands import find_valid_json
#from production.kaneru_io import get_recommendation_embeddings
from production.database_commands import get_all_subcategory_embeddings
from production.database_commands import get_book_details
from production.kaneru_io import get_all_inv_ids
from production.kaneru_io import get_embedding
from production.kaneru_io import has_image as io_has_image
from production.kaneru_io import get_embedded_categories
from production.kaneru_io import get_checksums
import production.redis_commands as cache

def redis_prefix(input_str: str) -> str:
    cleaned = input_str.lower()
    cleaned = cleaned.replace(' ', '_')
    for char in ["'", ",", "&"]:
        cleaned = cleaned.replace(char, '')
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    return cleaned


def blob_to_vec(blob, dim: int) -> np.ndarray:
    """
    Convert a MEDIUMBLOB (bytes or '0x..' hex string) into a float32 vector.
    dim: expected embedding dimension (e.g., 1536 for text-embedding-3-small)
    """
    if isinstance(blob, str):
        if blob.startswith("0x") or blob.startswith("0X"):
            data = bytes.fromhex(blob[2:])
        else:
            # if it's a plain hex string without 0x
            data = bytes.fromhex(blob)
    elif isinstance(blob, (bytes, bytearray, memoryview)):
        data = bytes(blob)
    else:
        raise TypeError(f"Unsupported blob type: {type(blob)}")

    # interpret as little-endian float32
    vec = np.frombuffer(data, dtype="<f4")  # '<f4' == little-endian float32
    if vec.size != dim:
        raise ValueError(f"Got {vec.size} floats; expected {dim}. Byte lenget_all_subcategory_embeddingsgth={len(data)}")
    return vec.astype(np.float32, copy=False)


def generate_category_embeddings(company_id):

    print("Generating Category Space.")
    subcategory_embedding_info = get_all_subcategory_embeddings()
        
    if subcategory_embedding_info is None:
        return None
                
    # Build arrays
    embeddings = [blob_to_vec(s['embedding'], 1536) for s in subcategory_embedding_info]
    subcategory_list = [s['subcategory'] for s in subcategory_embedding_info] 
    X = np.vstack(embeddings)      # shape (N, d)
   
    # Normalize for cosine similarity
    faiss.normalize_L2(X)

    # Build index
    d = X.shape[1]
    index_cpu = faiss.IndexFlatIP(d)   # inner product on unit vectors == cosine
    index_cpu.add(X)

    # Move to GPU (optional)
    res = faiss.StandardGpuResources()
    index_gpu = faiss.index_cpu_to_gpu(res, 0, index_cpu)

    inv_id_list = get_all_inv_ids(company_id)

    missing_embedding = []
    missing_cat = []
    threshold = 0.4
    results = {}
    cat_count = {}
    for inv_id in inv_id_list:
        embedding = get_embedding(inv_id)
        if embedding is None:
            missing_embedding.append(inv_id)
            continue
                 
        xq = embedding.reshape(1, -1).astype(np.float32)
        faiss.normalize_L2(xq)  # normalize query too
        
        D, I = index_gpu.search(xq, 10)
               
        candidates = {subcategory_list[idx]: float(dist) for idx, dist in zip(I[0], D[0])}
        found = False
        for cat in candidates.keys():
            if candidates[cat] > threshold:
                if not cat in results.keys():
                    results[cat] = []
                    cat_count[cat] = 0
                results[cat].append(inv_id)
                cat_count[cat] = cat_count[cat] + 1
                found = True
            else:
                break
        
        if not found:
            missing_cat.append(inv_id)
            
    for subcategory in results.keys():
        #key = "book_category_embedding_" + subcategory.replace(" ", "_")
        #cache.write_json(key, results[subcategory])
        book_list = []
        
        cached_inv_items = {}
        prefix = kp['CATEGORY_SEARCH']['key_prefix'] + redis_prefix(subcategory)
        status, data = cache.find_valid_json(prefix, 1440)
        if status:
            cached_inv_items = {item['inv_id']: item for item in data['items'] if 'inv_id' in item}
            inv_list = [item['inv_id'] for item in data['items'] if 'inv_id' in item]
            checksum_list = get_checksums(inv_list, company_id)
        
        for inv_id in results[subcategory]: 
            
            if inv_id in cached_inv_items:
                book = cached_inv_items[inv_id]
                if 'checksum' in book.keys():
                    cache_checksum = book['checksum']
                    valid_checksum = checksum_list[inv_id]
                    if cache_checksum == valid_checksum:
                        book_list.append(book)
                        continue
                    else:
                        print("############## No match ####################")
            book = get_book_details(inv_id)
            desc_id = book.pop('inv_desc_id')
            desc_str = base64.urlsafe_b64encode(desc_id).decode('utf-8').rstrip("=")
            has_image = io_has_image(desc_str)
            book['has_image'] = has_image
            if has_image:
                book['filename'] = desc_str
            else:
                book['has_image'] = True
                book['filename'] = 'blank'
            
            book_list.append(book)

        output = {'num_items' : len(book_list), 'items' : book_list }

        #prefix = kp['CATEGORY_SEARCH']['key_prefix'] + redis_prefix(subcategory) 
        cache.write_json(prefix, output)
        print(f"{prefix} : items ({len(results[subcategory])})") 
        
    category_map = get_embedded_categories()
    
    for category in category_map.keys():
        book_list = []
        for subcategory in category_map[category]:
            prefix = kp['CATEGORY_SEARCH']['key_prefix'] + redis_prefix(subcategory)
            status, data = cache.find_valid_json(prefix, 1440)
            if status:
                merged = []
                for item in book_list + data['items']:
                    if item not in merged:
                        merged.append(item)
                    book_list = merged
        
        output = {'num_items' : len(book_list), 'items' : book_list } 
               
        prefix = kp['CATEGORY_SEARCH']['key_prefix'] + redis_prefix(category) 
        cache.write_json(prefix, output)
        print(f"{prefix} : items ({len(output)})")                
                 
    print(f"Missing Embeddings: {len(missing_embedding)}")    
    print(f"Missing Categories: {len(missing_cat)}")          
    print("Completed Category Space.")
    
    return cat_count
    
#generate_recommendation_embeddings(5,1)
#output = user_recommendation_list()
#print(output)



