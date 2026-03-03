import numpy as np

def age(now_year, publish_year, A=1.0, k=3.0, B=0.1, s=0.02):

    if isinstance(now_year, str):
        now_year = extract_year(now_year)
        
    if isinstance(publish_year, str):
        publish_year = extract_year(publish_year)
               
    age = now_year - publish_year
    newness = A * np.exp(-k * age)
    scarcity = B * (1 + s)**age
    return 0.9 * (newness + scarcity) # 0.9 normalizes to 1 at year 0 


def condition(c):

    factor = 0.5
    if c.lower() == "brand new":
        factor = 1.0
    elif c.lower() == "very good":
        factor = 0.75
    elif c.lower() == "good":
        factor = 0.5
    elif c.lower() == "acceptable":
        factor = 0.25 
    elif c.lower() == "poor":
        factor = 0.1 
        
    return factor

def book_format(is_paperback):

    if is_paperback:
        return 0.5
    else:
        return 1.0

def estimate(now_year, latent_price, book_data):
   
    ### P_i = Format(f_i) × Condition(c_i) × AgeValue(a_i) × P_latent  
    book_year = book_data["publish_year"]
    is_paperback = (book_data["format"].lower() == "paperback")
    book_condition = book_data["condition"]
   
    price = latent_price*book_format(is_paperback)*age(now_year, book_year)*condition(book_condition)
    
    return price
    
    
def remove_outliers_iqr(data):
    data = np.array(data)
    q1 = np.percentile(data, 25)
    q3 = np.percentile(data, 60)
    iqr = q3 - q1
    
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    filtered = data[(data >= lower_bound) & (data <= upper_bound)]
    return filtered.tolist()    

def extract_year(date_str):
    if date_str is None:
        return None
    
    parts = date_str.split("-")
    
    for part in parts:
        part = part.strip()
        if len(part) == 4 and part.isdigit():
            return int(part)
    
    parts = date_str.split(" ")
    for part in parts:
        part = part.strip()
        if len(part) == 4 and part.isdigit():
            return int(part)
                
    return None  # No valid year found

def estimate_latent(now_year, data, isbn):

    average_latent_price = 0
    num_points = len(data)
    if num_points == 0:
       return None
 
    if num_points < 3: #add logic when isbn is none
        count = 0
        for d in data:
            d_isbn = d["isbn"]
            if not d_isbn is None:
                if isbn == d_isbn:
                    price = float(d['price'])
                    average_latent_price += price/(book_format(True)*age(now_year,  d['publish_date'])*condition(d["condition"]))				        
                    count +=1
        if count == 0:
            return None
        else:
            return average_latent_price/count
     
    latent_prices = []
    average_latent_price = 0 
    
    for d in data:
        ### P_i = Format(f_i) × Condition(c_i) × AgeValue(a_i) × P_latent
        price = float(d['price'])
        is_paperback = (d["format"].lower() == "paperback")
        latent = price/(book_format(is_paperback)*age(now_year, d['publish_date'])*condition(d["condition"]))
        latent_prices.append(latent)
        #print(f"{d['format']}, {d['condition']} : ${price}, ${latent}")

    latent_prices = remove_outliers_iqr(latent_prices)
    #for l in latent_prices:
    #    print(l)
    average_latent_price = average = sum(latent_prices) / len(latent_prices)   
    
    #throw_out_outliers
    
    ### check ccy!!!    
        
    return average_latent_price
    

    
