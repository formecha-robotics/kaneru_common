import faiss
import numpy as np
from production.cache_keys import keys_and_policy as kp
from production.redis_commands import write_json
from production.redis_commands import find_valid_json
from production.kaneru_io import get_recommendation_embeddings

def user_recommendation_list(venue_id, company_id, user_id):

    output = get_recommendation_list([1602])

    return [item for item in output.keys()] 


def get_recommendation_list(pub_list, required_num=50, exclude_list=[]):

    output = {}  # pub_id -> weight (float)

    for pub_id in pub_list:
        key = kp['ECOMM_RECOMMENDATIONS']['key_prefix'] + str(pub_id)
        policy = kp['ECOMM_RECOMMENDATIONS']['expiry_policy']
        status, recs = find_valid_json(key, policy)
        if not status or not recs:
            continue

        # Merge keeping the highest weight per key
        for k, v in recs.items():
            try:
                # ensure numeric weight
                w = float(v)
            except (TypeError, ValueError):
                continue
            # keep max weight if duplicate
            if k not in exclude_list:
                if k in output:
                    if w > output[k]:
                        output[k] = w
                else:
                    output[k] = w
                
        if len(output.keys()) >= required_num:
            break    

    output = {k: v for k, v in sorted(output.items(), key=lambda x: x[1], reverse=True)}
    
    num = len(output.keys())
    previous_num = num
    
    exclude_list = exclude_list + pub_list
     
    while num < required_num:

        new_pub_list = []
        for pub_id in output.keys():
            if not pub_id in pub_list:
                new_pub_list.append(pub_id)
    
        if len(new_pub_list) == 0:
            return output
    
        
        next_recs = get_recommendation_list(new_pub_list, (required_num - num), exclude_list)
        exclude_list = exclude_list + new_pub_list
        
        output = {**next_recs, **output}
        num = len(output.keys())
        if num == previous_num:
            break
        previous_num = num
    
    return output

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
        raise ValueError(f"Got {vec.size} floats; expected {dim}. Byte length={len(data)}")
    return vec.astype(np.float32, copy=False)


def generate_recommendation_embeddings(venue_id, company_id):

    print("Generating recommendation space.")
    embeddings = get_recommendation_embeddings(venue_id, company_id)

    # Build arrays
    pub_ids = list(embeddings.keys())                        # list of all pub_ids
    X = np.vstack([embeddings[pid] for pid in pub_ids])      # shape (N, d)
    X = X.astype(np.float32)

    # Normalize for cosine similarity
    faiss.normalize_L2(X)

    # Build index
    d = X.shape[1]
    index_cpu = faiss.IndexFlatIP(d)   # inner product on unit vectors == cosine
    index_cpu.add(X)

    # Move to GPU (optional)
    res = faiss.StandardGpuResources()
    index_gpu = faiss.index_cpu_to_gpu(res, 0, index_cpu)

    # Query: pick one pub_id, get its neighbors
    for query_pub_id in pub_ids:
        xq = embeddings[query_pub_id].reshape(1, -1).astype(np.float32)
        faiss.normalize_L2(xq)  # normalize query too

        D, I = index_gpu.search(xq, 11)

        # Map FAISS indices back to pub_ids
        neighbors = {pub_ids[idx]: float(dist) for idx, dist in zip(I[0], D[0])}
        del neighbors[query_pub_id]
        
        key = kp['ECOMM_RECOMMENDATIONS']['key_prefix'] + str(query_pub_id)
        write_json(key, neighbors)

        #print("Neighbors for", query_pub_id, ":", neighbors)

    print("Completed recommendation space.")
    
#generate_recommendation_embeddings(5,1)
#output = user_recommendation_list()
#print(output)


